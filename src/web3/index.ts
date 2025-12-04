import { parseAbi } from "viem"
import { ChainId, publicClients } from "../config/web3"
import WalletHolding from "../models/WalletHolding"
import { SUPPORTED_ASSETS } from "../constants"

Object.entries(publicClients).map(([chainId, client]) => {
  client.watchEvent({
    events: parseAbi([
      "event Deposited(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
      "event Withdrawn(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
      "event Borrowed(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
      "event Repaid(address indexed user, address indexed token, uint256 amount, uint256 nonce)",
      "event Liquidated(address indexed liquidator, address indexed user, address indexed collateralToken, uint256 collateralAmount, address debtToken, uint256 debtRepaid, uint256 nonce)",
    ]),
    onLogs: async (logs) => {
      for (const log of logs) {
        if (log.eventName === "Deposited") {
          const { user, token, amount, nonce } = log.args

          const asset = SUPPORTED_ASSETS[Number(chainId) as ChainId].find(
            (item) => item.address.toLowerCase() === token?.toLowerCase()
          )
          if (!asset || !token) continue
          if (!token) continue
          let walletHolding = await WalletHolding.findOne({ wallet: user })
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              deposits: [
                {
                  chain: Number(chainId),
                  symbol: token,
                  amount: amount ?? 0n,
                },
              ],
            })
            await walletHolding.save()
          } else {
            const depositIndex = walletHolding.deposits.findIndex(
              (deposit) =>
                deposit.chain === Number(chainId) &&
                deposit.symbol.toLowerCase() === token?.toLowerCase()
            )
            if (depositIndex !== -1) {
              walletHolding.deposits[depositIndex].amount =
                walletHolding.deposits[depositIndex].amount + (amount ?? 0n)
            } else {
              walletHolding.deposits.push({
                chain: Number(chainId),
                symbol: token!,
                amount: amount ?? 0n,
              })
            }
            await walletHolding.save()
          }
        } else if (log.eventName === "Withdrawn") {
          const { user, token, amount, nonce } = log.args
          if (!token) continue
          let walletHolding = await WalletHolding.findOne({ wallet: user })
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              deposits: [
                {
                  chain: Number(chainId),
                  symbol: token,
                  amount: -(amount ?? 0n),
                },
              ],
            })
            await walletHolding.save()
          } else {
            const depositIndex = walletHolding.deposits.findIndex(
              (deposit) =>
                deposit.chain === Number(chainId) &&
                deposit.symbol.toLowerCase() === token?.toLowerCase()
            )
            if (depositIndex !== -1) {
              walletHolding.deposits[depositIndex].amount =
                walletHolding.deposits[depositIndex].amount - (amount ?? 0n)
            } else {
              walletHolding.deposits.push({
                chain: Number(chainId),
                symbol: token!,
                amount: -(amount ?? 0n),
              })
            }
            await walletHolding.save()
          }
        } else if (log.eventName === "Borrowed") {
          const { user, token, amount, nonce } = log.args
          if (!token) continue
          let walletHolding = await WalletHolding.findOne({ wallet: user })
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              borrows: [
                {
                  chain: Number(chainId),
                  symbol: token,
                  amount: amount ?? 0n,
                },
              ],
            })
            await walletHolding.save()
          } else {
            const borrowIndex = walletHolding.borrows.findIndex(
              (borrow) =>
                borrow.chain === Number(chainId) &&
                borrow.symbol.toLowerCase() === token?.toLowerCase()
            )
            if (borrowIndex !== -1) {
              walletHolding.borrows[borrowIndex].amount =
                walletHolding.borrows[borrowIndex].amount + (amount ?? 0n)
            } else {
              walletHolding.borrows.push({
                chain: Number(chainId),
                symbol: token!,
                amount: amount ?? 0n,
              })
            }
            await walletHolding.save()
          }
        } else if (log.eventName === "Repaid") {
          const { user, token, amount, nonce } = log.args
          if (!token) continue
          let walletHolding = await WalletHolding.findOne({ wallet: user })
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              borrows: [
                {
                  chain: Number(chainId),
                  symbol: token,
                  amount: -(amount ?? 0n),
                },
              ],
            })
            await walletHolding.save()
          } else {
            const borrowIndex = walletHolding.borrows.findIndex(
              (borrow) =>
                borrow.chain === Number(chainId) &&
                borrow.symbol.toLowerCase() === token?.toLowerCase()
            )
            if (borrowIndex !== -1) {
              walletHolding.borrows[borrowIndex].amount =
                walletHolding.borrows[borrowIndex].amount - (amount ?? 0n)
            } else {
              walletHolding.borrows.push({
                chain: Number(chainId),
                symbol: token!,
                amount: -(amount ?? 0n),
              })
            }
            await walletHolding.save()
          }
        } else if (log.eventName === "Liquidated") {
          const {
            user,
            collateralToken,
            collateralAmount,
            debtToken,
            debtRepaid,
            nonce,
          } = log.args
          if (!collateralToken || !debtToken) continue
          let walletHolding = await WalletHolding.findOne({ wallet: user })
          if (!walletHolding) {
            walletHolding = new WalletHolding({
              wallet: user,
              borrows: [
                {
                  chain: Number(chainId),
                  symbol: debtToken,
                  amount: -(debtRepaid ?? 0n),
                },
              ],
              deposits: [
                {
                  chain: Number(chainId),
                  symbol: collateralToken,
                  amount: -(collateralAmount ?? 0n),
                },
              ],
            })
            await walletHolding.save()
          } else {
            const borrowIndex = walletHolding.borrows.findIndex(
              (borrow) =>
                borrow.chain === Number(chainId) &&
                borrow.symbol.toLowerCase() === debtToken?.toLowerCase()
            )
            if (borrowIndex !== -1) {
              walletHolding.borrows[borrowIndex].amount =
                walletHolding.borrows[borrowIndex].amount - (debtRepaid ?? 0n)
            } else {
              walletHolding.borrows.push({
                chain: Number(chainId),
                symbol: debtToken!,
                amount: -(debtRepaid ?? 0n),
              })
            }
            const depositIndex = walletHolding.deposits.findIndex(
              (deposit) =>
                deposit.chain === Number(chainId) &&
                deposit.symbol.toLowerCase() === collateralToken?.toLowerCase()
            )
            if (depositIndex !== -1) {
              walletHolding.deposits[depositIndex].amount =
                walletHolding.deposits[depositIndex].amount -
                (collateralAmount ?? 0n)
            } else {
              walletHolding.deposits.push({
                chain: Number(chainId),
                symbol: collateralToken!,
                amount: -(collateralAmount ?? 0n),
              })
            }
            await walletHolding.save()
          }
        }
      }
    },
    batch: true,
    poll: true,
    pollingInterval: 750,
  })
})
