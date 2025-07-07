using System.Data;
using Milimoe.FunGame.Core.Api.Transmittal;
using Milimoe.FunGame.Core.Api.Utility;
using Milimoe.FunGame.Core.Entity;
using Milimoe.FunGame.Core.Library.Constant;
using Milimoe.FunGame.Core.Library.SQLScript.Entity;

namespace ProjectRedbud.FunGame.SQLQueryExtension
{
    public static class MarketQueryExtension
    {
        public static MarketItem? GetMarketItem(this SQLHelper helper, Guid itemGuid)
        {
            DataRow? dr = helper.ExecuteDataRow(MarketItemsQuery.Select_MarketItemsByItemGuid(helper, itemGuid));
            if (dr != null)
            {
                MarketItem marketItem = new();
                SetValue(helper, dr, marketItem);
                return marketItem;
            }
            return null;
        }

        public static List<MarketItem> GetMarketItemsByItemGuid(this SQLHelper helper, Guid itemGuid)
        {
            List<MarketItem> MarketItem = [];
            DataSet ds = helper.ExecuteDataSet(MarketItemsQuery.Select_MarketItemsByItemGuid(helper, itemGuid));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    MarketItem marketItem = new();
                    SetValue(helper, dr, marketItem);
                    MarketItem.Add(marketItem);
                }
            }
            return MarketItem;
        }

        public static List<MarketItem> GetMarketItemsByUserId(this SQLHelper helper, long userId)
        {
            List<MarketItem> MarketItem = [];
            DataSet ds = helper.ExecuteDataSet(MarketItemsQuery.Select_MarketItemsByUserId(helper, userId));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    MarketItem marketItem = new();
                    SetValue(helper, dr, marketItem);
                    MarketItem.Add(marketItem);
                }
            }
            return MarketItem;
        }

        public static List<MarketItem> GetMarketItemsByState(this SQLHelper helper, MarketItemState state)
        {
            List<MarketItem> MarketItem = [];
            DataSet ds = helper.ExecuteDataSet(MarketItemsQuery.Select_MarketItemsByState(helper, state));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    MarketItem marketItem = new();
                    SetValue(helper, dr, marketItem);
                    MarketItem.Add(marketItem);
                }
            }
            return MarketItem;
        }

        public static List<MarketItem> GetAllMarketsItem(this SQLHelper helper, long userId = 0, MarketItemState? state = null)
        {
            List<MarketItem> MarketItem = [];
            DataSet ds = helper.ExecuteDataSet(MarketItemsQuery.Select_AllMarketItems(helper, userId, state));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    MarketItem marketItem = new();
                    SetValue(helper, dr, marketItem);
                    MarketItem.Add(marketItem);
                }
            }
            return MarketItem;
        }

        public static void AddMarketItem(this SQLHelper helper, Guid itemGuid, long userId, double price, MarketItemState state = MarketItemState.Listed)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Insert_MarketItem(helper, itemGuid, userId, price, state));
                if (!helper.Success) throw new Exception($"新增市场物品 {itemGuid} 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemPrice(this SQLHelper helper, Guid itemGuid, double price)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_MarketItemPrice(helper, itemGuid, price));
                if (!helper.Success) throw new Exception($"更新市场物品 {itemGuid} 的价格失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemState(this SQLHelper helper, Guid itemGuid, MarketItemState state)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_MarketItemState(helper, itemGuid, state));
                if (!helper.Success) throw new Exception($"更新市场物品 {itemGuid} 状态失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemBuyer(this SQLHelper helper, Guid itemGuid, long buyer)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_Buy(helper, itemGuid, buyer));
                if (!helper.Success) throw new Exception($"更新市场物品 {itemGuid} 的买家 {buyer} 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemFinishTime(this SQLHelper helper, Guid itemGuid, DateTime finishTime)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_MarketItemFinishTime(helper, itemGuid, finishTime));
                if (!helper.Success) throw new Exception($"更新市场物品 {itemGuid} 交易完成时间 {finishTime} 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteMarketItem(this SQLHelper helper, Guid itemGuid)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Delete_MarketItem(helper, itemGuid));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteMarketItemByUserId(this SQLHelper helper, long userid)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Delete_MarketItemByUserId(helper, userid));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        private static void SetValue(SQLHelper helper, DataRow dr, MarketItem marketItem)
        {
            marketItem.Id = (long)dr[MarketItemsQuery.Column_Id];
            long userid = (long)dr[MarketItemsQuery.Column_UserId];
            marketItem.User = helper.GetUserById(userid) ?? Factory.GetUser(userid);
            if (marketItem.User.Inventory.Items.FirstOrDefault(i => i.Guid.ToString().EqualsGuid(dr[MarketItemsQuery.Column_ItemGuid])) is Item item)
            {
                marketItem.Item = item;
            }
            marketItem.Price = (double)dr[MarketItemsQuery.Column_Price];

            if (dr[MarketItemsQuery.Column_CreateTime] != DBNull.Value && DateTime.TryParseExact(dr[MarketItemsQuery.Column_CreateTime].ToString(), General.GeneralDateTimeFormat, null, System.Globalization.DateTimeStyles.None, out DateTime dt))
            {
                marketItem.CreateTime = dt;
            }
            
            if (dr[MarketItemsQuery.Column_FinishTime] != DBNull.Value && DateTime.TryParseExact(dr[MarketItemsQuery.Column_FinishTime].ToString(), General.GeneralDateTimeFormat, null, System.Globalization.DateTimeStyles.None, out dt))
            {
                marketItem.FinishTime = dt;
            }

            marketItem.Status = (MarketItemState)Convert.ToInt32(dr[MarketItemsQuery.Column_Status]);

            long buyerid = (long)dr[MarketItemsQuery.Column_Buyer];
            marketItem.Buyer = helper.GetUserById(buyerid) ?? Factory.GetUser(buyerid);
        }
    }
}
