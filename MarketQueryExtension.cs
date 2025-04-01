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
        public static MarketItem? GetMarketItem(this SQLHelper helper, long marketItemId)
        {
            DataRow? dr = helper.ExecuteDataRow(MarketItemsQuery.Select_MarketItemById(helper, marketItemId));
            if (dr != null)
            {
                MarketItem marketItem = new();
                SetValue(helper, dr, marketItem);
                return marketItem;
            }
            return null;
        }

        public static List<MarketItem> GetMarketItemsByItemId(this SQLHelper helper, long itemId)
        {
            List<MarketItem> MarketItem = [];
            DataSet ds = helper.ExecuteDataSet(MarketItemsQuery.Select_MarketItemsByItemId(helper, itemId));
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

        public static List<MarketItem> GetAllMarketsItem(this SQLHelper helper, long itemId = 0, long userId = 0, MarketItemState? state = null)
        {
            List<MarketItem> MarketItem = [];
            DataSet ds = helper.ExecuteDataSet(MarketItemsQuery.Select_AllMarketItems(helper, itemId, userId, state));
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

        public static void AddMarketItem(this SQLHelper helper, long itemId, long userId, double price, MarketItemState state = MarketItemState.Listed)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Insert_MarketItem(helper, itemId, userId, price, state));
                if (!helper.Success) throw new Exception($"新增市场物品 {itemId} 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemPrice(this SQLHelper helper, long id, double price)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_MarketItemPrice(helper, id, price));
                if (!helper.Success) throw new Exception($"更新市场物品 {id} 的价格失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemState(this SQLHelper helper, long id, MarketItemState state)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_MarketItemState(helper, id, state));
                if (!helper.Success) throw new Exception($"更新市场物品 {id} 状态失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemBuyer(this SQLHelper helper, long id, long buyer)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_MarketItemBuyer(helper, id, buyer));
                if (!helper.Success) throw new Exception($"更新市场物品 {id} 的买家 {buyer} 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateMarketItemFinishTime(this SQLHelper helper, long id, DateTime finishTime)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Update_MarketItemFinishTime(helper, id, finishTime));
                if (!helper.Success) throw new Exception($"更新市场物品 {id} 交易完成时间 {finishTime} 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteMarketItem(this SQLHelper helper, long id)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(MarketItemsQuery.Delete_MarketItem(helper, id));

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
            marketItem.Item = Factory.OpenFactory.GetInstance<Item>((long)dr[MarketItemsQuery.Column_ItemId], "", []);
            long userid = (long)dr[MarketItemsQuery.Column_UserId];
            marketItem.User = helper.GetUserById(userid) ?? Factory.GetUser(userid);
            marketItem.Price = (double)dr[MarketItemsQuery.Column_Price];
            marketItem.CreateTime = (DateTime)dr[MarketItemsQuery.Column_CreateTime];

            if (dr[MarketItemsQuery.Column_FinishTime] != DBNull.Value)
            {
                marketItem.FinishTime = (DateTime)dr[MarketItemsQuery.Column_FinishTime];
            }

            marketItem.Status = (MarketItemState)(int)dr[MarketItemsQuery.Column_Status];

            long buyerid = (long)dr[MarketItemsQuery.Column_Buyer];
            marketItem.Buyer = helper.GetUserById(buyerid) ?? Factory.GetUser(buyerid);
        }
    }
}
