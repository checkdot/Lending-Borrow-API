import { CronJob } from "cron"
import PoolsRewards from "../models/PoolsRewards"
import WalletHolding from "../models/WalletHolding"

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
