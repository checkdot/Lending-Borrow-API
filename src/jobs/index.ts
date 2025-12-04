import { CronJob } from "cron"
import PoolsRewards from "../models/PoolsRewards"
import WalletHolding from "../models/WalletHolding"
import { ChainId, GET_LOGS_BLOCKS, publicClients } from "../config/web3"
import Event from "../models/Event"
import { parseAbi } from "viem"
import {
  LENDING_CONTRACT_ADDRS,
  SUPPORTED_ASSETS,
  START_BLOCK_NUMBERS,
} from "../constants"

new CronJob(
  "0 0 0 * * *",
  async () => {
    const currentTime =
      Math.floor(Date.now() / (1000 * 60 * 60 * 24)) * (1000 * 60 * 60 * 24)

    const poolReward = await PoolsRewards.findOne({
      startDate: { $lte: currentTime },
      endDate: { $gte: currentTime },
    })
    if (!poolReward) return

    const type = poolReward.type === "deposit" ? "deposits" : "borrows"
    const walletHoldings = await WalletHolding.find({
      [type]: {
        $elemMatch: {
          chain: poolReward.chain,
          symbol: poolReward.symbol,
          amount: { $gt: 0n },
        },
      },
    })
    const dailyReward =
      poolReward.totalReward.quantity /
      BigInt(
        Math.floor(
          (poolReward.endDate - poolReward.startDate) / (1000 * 60 * 60 * 24)
        )
      )

    const parts = walletHoldings.map((walletHolding) => {
      return {
        wallet: walletHolding.wallet,
        amount:
          walletHolding?.[type]?.find(
            (item) =>
              item.chain === poolReward.chain &&
              item.symbol === poolReward.symbol
          )?.amount ?? 0n,
      }
    })

    const totalParts = parts.reduce((acc, part) => acc + part.amount, 0n)
    if (totalParts === 0n) return

    const bulkOps = walletHoldings.map((walletHolding, i) => {
      const rewardAmount = (dailyReward * parts[i].amount) / totalParts
      return {
        updateOne: {
          filter: { _id: walletHolding._id },
          update: {
            $push: {
              rewards: {
                chain: poolReward.chain,
                symbol: poolReward.symbol,
                amount: rewardAmount,
                date: currentTime,
              },
            },
          },
        },
      }
    })

    if (bulkOps.length > 0) {
      await WalletHolding.bulkWrite(bulkOps)
    }
  },
  null,
  true
)

new CronJob(
  "*/12 * * * * *",
  async () => {
    for (const [chainId, client] of Object.entries(publicClients)) {
      const lastEvent = await Event.findOne({
        chain: Number(chainId),
      }).sort({ blockNumber: -1, nonce: -1 })
      const lastBlockNumber =
        lastEvent?.blockNumber ??
        START_BLOCK_NUMBERS[Number(chainId) as ChainId]
      const lastNonce = lastEvent?.nonce ?? 0n

      const newEvents = (
        await client.getLogs({
          address: LENDING_CONTRACT_ADDRS[
            Number(chainId) as ChainId
          ] as `0x${string}`,
          fromBlock: lastBlockNumber,
          toBlock:
            lastBlockNumber + GET_LOGS_BLOCKS[Number(chainId) as ChainId],
          events: parseAbi([
            "event Deposited(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
            "event Withdrawn(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
            "event Borrowed(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
            "event Repaid(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
            "event Liquidated(address indexed liquidator, address indexed user, address indexed collateralToken, uint256 collateralAmount, address debtToken, uint256 debtRepaid, uint256 nonce)",
          ]),
        })
      ).filter((log) => (log.args.nonce ?? 0n) > lastNonce)

      if (newEvents.length === 0) continue

      // Collect all unique wallet addresses
      const walletAddresses = new Set<string>()
      for (const log of newEvents) {
        const user = log.args.user as string | undefined
        if (user) walletAddresses.add(user.toLowerCase())
      }

      // Fetch all existing wallet holdings in one query
      const existingHoldings = await WalletHolding.find({
        wallet: { $in: Array.from(walletAddresses) },
      })
      const holdingsMap = new Map<string, (typeof existingHoldings)[0]>()
      const existingWalletSet = new Set<string>()
      for (const holding of existingHoldings) {
        const walletLower = holding.wallet.toLowerCase()
        holdingsMap.set(walletLower, holding)
        existingWalletSet.add(walletLower)
      }

      // Process all events and update in-memory objects
      for (const log of newEvents) {
        if (log.eventName === "Deposited") {
          const { user, token, amount } = log.args

          const asset = SUPPORTED_ASSETS[Number(chainId) as ChainId].find(
            (item) => item.address.toLowerCase() === token?.toLowerCase()
          )
          if (!asset || !token) continue

          const userLower = user?.toLowerCase()
          if (!userLower) continue

          let walletHolding = holdingsMap.get(userLower)
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              deposits: [],
              borrows: [],
              rewards: [],
            })
            holdingsMap.set(userLower, walletHolding)
          }

          const depositIndex = walletHolding.deposits.findIndex(
            (deposit) =>
              deposit.chain === Number(chainId) &&
              deposit.symbol.toLowerCase() === token.toLowerCase()
          )
          if (depositIndex !== -1) {
            walletHolding.deposits[depositIndex].amount =
              walletHolding.deposits[depositIndex].amount + (amount ?? 0n)
          } else {
            walletHolding.deposits.push({
              chain: Number(chainId),
              symbol: token,
              amount: amount ?? 0n,
            })
          }
        } else if (log.eventName === "Withdrawn") {
          const { user, token, amount } = log.args
          if (!token) continue

          const userLower = user?.toLowerCase()
          if (!userLower) continue

          let walletHolding = holdingsMap.get(userLower)
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              deposits: [],
              borrows: [],
              rewards: [],
            })
            holdingsMap.set(userLower, walletHolding)
          }

          const depositIndex = walletHolding.deposits.findIndex(
            (deposit) =>
              deposit.chain === Number(chainId) &&
              deposit.symbol.toLowerCase() === token.toLowerCase()
          )
          if (depositIndex !== -1) {
            walletHolding.deposits[depositIndex].amount =
              walletHolding.deposits[depositIndex].amount - (amount ?? 0n)
          } else {
            walletHolding.deposits.push({
              chain: Number(chainId),
              symbol: token,
              amount: -(amount ?? 0n),
            })
          }
        } else if (log.eventName === "Borrowed") {
          const { user, token, amount } = log.args
          if (!token) continue

          const userLower = user?.toLowerCase()
          if (!userLower) continue

          let walletHolding = holdingsMap.get(userLower)
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              deposits: [],
              borrows: [],
              rewards: [],
            })
            holdingsMap.set(userLower, walletHolding)
          }

          const borrowIndex = walletHolding.borrows.findIndex(
            (borrow) =>
              borrow.chain === Number(chainId) &&
              borrow.symbol.toLowerCase() === token.toLowerCase()
          )
          if (borrowIndex !== -1) {
            walletHolding.borrows[borrowIndex].amount =
              walletHolding.borrows[borrowIndex].amount + (amount ?? 0n)
          } else {
            walletHolding.borrows.push({
              chain: Number(chainId),
              symbol: token,
              amount: amount ?? 0n,
            })
          }
        } else if (log.eventName === "Repaid") {
          const { user, token, amount } = log.args
          if (!token) continue

          const userLower = user?.toLowerCase()
          if (!userLower) continue

          let walletHolding = holdingsMap.get(userLower)
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              deposits: [],
              borrows: [],
              rewards: [],
            })
            holdingsMap.set(userLower, walletHolding)
          }

          const borrowIndex = walletHolding.borrows.findIndex(
            (borrow) =>
              borrow.chain === Number(chainId) &&
              borrow.symbol.toLowerCase() === token.toLowerCase()
          )
          if (borrowIndex !== -1) {
            walletHolding.borrows[borrowIndex].amount =
              walletHolding.borrows[borrowIndex].amount - (amount ?? 0n)
          } else {
            walletHolding.borrows.push({
              chain: Number(chainId),
              symbol: token,
              amount: -(amount ?? 0n),
            })
          }
        } else if (log.eventName === "Liquidated") {
          const {
            user,
            collateralToken,
            collateralAmount,
            debtToken,
            debtRepaid,
          } = log.args
          if (!collateralToken || !debtToken) continue

          const userLower = user?.toLowerCase()
          if (!userLower) continue

          let walletHolding = holdingsMap.get(userLower)
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              deposits: [],
              borrows: [],
              rewards: [],
            })
            holdingsMap.set(userLower, walletHolding)
          }

          const borrowIndex = walletHolding.borrows.findIndex(
            (borrow) =>
              borrow.chain === Number(chainId) &&
              borrow.symbol.toLowerCase() === debtToken.toLowerCase()
          )
          if (borrowIndex !== -1) {
            walletHolding.borrows[borrowIndex].amount =
              walletHolding.borrows[borrowIndex].amount - (debtRepaid ?? 0n)
          } else {
            walletHolding.borrows.push({
              chain: Number(chainId),
              symbol: debtToken,
              amount: -(debtRepaid ?? 0n),
            })
          }

          const depositIndex = walletHolding.deposits.findIndex(
            (deposit) =>
              deposit.chain === Number(chainId) &&
              deposit.symbol.toLowerCase() === collateralToken.toLowerCase()
          )
          if (depositIndex !== -1) {
            walletHolding.deposits[depositIndex].amount =
              walletHolding.deposits[depositIndex].amount -
              (collateralAmount ?? 0n)
          } else {
            walletHolding.deposits.push({
              chain: Number(chainId),
              symbol: collateralToken,
              amount: -(collateralAmount ?? 0n),
            })
          }
        }
      }

      // Build bulk operations
      const bulkOps: any[] = []
      for (const [walletLower, walletHolding] of holdingsMap.entries()) {
        if (existingWalletSet.has(walletLower)) {
          // Update existing
          bulkOps.push({
            updateOne: {
              filter: { _id: walletHolding._id },
              update: {
                $set: {
                  deposits: walletHolding.deposits,
                  borrows: walletHolding.borrows,
                },
              },
            },
          })
        } else {
          // Insert new - convert Mongoose document to plain object
          const doc = walletHolding.toObject
            ? walletHolding.toObject()
            : walletHolding
          bulkOps.push({
            insertOne: {
              document: {
                wallet: doc.wallet,
                deposits: doc.deposits || [],
                borrows: doc.borrows || [],
                rewards: [],
                tvl: 0,
                apy: 0,
              },
            },
          })
        }
      }

      // Execute bulk write
      if (bulkOps.length > 0) {
        await WalletHolding.bulkWrite(bulkOps)
      }
      await Event.insertMany(
        newEvents.map((log) => ({
          type: log.eventName,
          chain: Number(chainId),
          user: log.args.user as string,
          token: (log.args as { token?: `0x${string}` }).token ?? undefined,
          amount: (log.args as { amount?: bigint }).amount ?? undefined,
          nonce: (log.args as { nonce?: bigint }).nonce ?? 0n,
          blockNumber: log.blockNumber,
          liquidator:
            (log.args as { liquidator?: `0x${string}` }).liquidator ??
            undefined,
          collateralToken:
            (log.args as { collateralToken?: `0x${string}` }).collateralToken ??
            undefined,
          collateralAmount:
            (log.args as { collateralAmount?: bigint }).collateralAmount ??
            undefined,
          debtToken:
            (log.args as { debtToken?: `0x${string}` }).debtToken ?? undefined,
          debtRepaid:
            (log.args as { debtRepaid?: bigint }).debtRepaid ?? undefined,
        }))
      )
    }
  },
  null,
  true
)
