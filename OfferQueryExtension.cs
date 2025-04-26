using System.Data;
using Milimoe.FunGame.Core.Api.Transmittal;
using Milimoe.FunGame.Core.Api.Utility;
using Milimoe.FunGame.Core.Entity;
using Milimoe.FunGame.Core.Library.Constant;
using Milimoe.FunGame.Core.Library.SQLScript.Entity;

namespace ProjectRedbud.FunGame.SQLQueryExtension
{
    public static class OfferQueryExtension
    {
        public static Offer? GetOffer(this SQLHelper helper, long offerId, bool isBackup = false)
        {
            DataRow? dr = helper.ExecuteDataRow(OffersQuery.Select_OfferById(helper, offerId));
            if (dr != null)
            {
                Offer offer = new();
                SetValue(helper, dr, offer, isBackup);
                return offer;
            }
            return null;
        }

        public static List<Offer> GetOffersByOfferor(this SQLHelper helper, long offerorId, bool isBackup = false)
        {
            List<Offer> offers = [];
            DataSet ds = helper.ExecuteDataSet(OffersQuery.Select_OffersByOfferor(helper, offerorId));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    Offer offer = new();
                    SetValue(helper, dr, offer, isBackup);
                    offers.Add(offer);
                }
            }
            return offers;
        }

        public static List<Offer> GetOffersByOfferee(this SQLHelper helper, long offereeId, bool isBackup = false)
        {
            List<Offer> offers = [];
            DataSet ds = helper.ExecuteDataSet(OffersQuery.Select_OffersByOfferee(helper, offereeId));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    Offer offer = new();
                    SetValue(helper, dr, offer, isBackup);
                    offers.Add(offer);
                }
            }
            return offers;
        }

        public static List<Guid> GetOfferItemsByOfferIdAndUserId(this SQLHelper helper, long offerId, long userId)
        {
            List<Guid> itemGuids = [];
            User? user = helper.GetUserById(userId);
            DataSet ds = helper.ExecuteDataSet(OfferItemsQuery.Select_OfferItemsByOfferIdAndUserId(helper, offerId, userId));
            if (user != null && helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    if (user.Inventory.Items.FirstOrDefault(i => i.Guid.ToString().EqualsGuid(dr[OfferItemsQuery.Column_ItemGuid])) is Item item)
                    {
                        itemGuids.Add(item.Guid);
                    }
                }
            }
            return itemGuids;
        }

        public static List<Guid> GetOfferItemsBackupByOfferIdAndUserId(this SQLHelper helper, long offerId, long userId)
        {
            List<Guid> itemGuids = [];
            User? user = helper.GetUserById(userId);
            DataSet ds = helper.ExecuteDataSet(OfferItemsQuery.Select_OfferItemsBackupByOfferIdAndUserId(helper, offerId, userId));
            if (user != null && helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    if (user.Inventory.Items.FirstOrDefault(i => i.Guid.ToString().EqualsGuid(dr[OfferItemsQuery.Column_ItemGuid])) is Item item)
                    {
                        itemGuids.Add(item.Guid);
                    }
                }
            }
            return itemGuids;
        }

        public static void AddOffer(this SQLHelper helper, long offeror, long offeree, OfferState status = OfferState.Created, int negotiatedTimes = 0)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OffersQuery.Insert_Offer(helper, offeror, offeree, status, negotiatedTimes));
                if (!helper.Success) throw new Exception($"新增报价 (Offeror: {offeror}, Offeree: {offeree}) 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void AddOfferItem(this SQLHelper helper, long offerId, long userId, Guid itemGuid)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OfferItemsQuery.Insert_OfferItem(helper, offerId, userId, itemGuid));
                if (!helper.Success) throw new Exception($"新增报价物品 (OfferId: {offerId}, UserId: {userId}, ItemGuid: {itemGuid}) 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void BackupOfferItem(this SQLHelper helper, Offer offer)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OfferItemsQuery.Delete_OfferItemsBackupByOfferId(helper, offer.Id));
                foreach (Guid itemGuid in offer.OfferorItems)
                {
                    helper.Execute(OfferItemsQuery.Insert_OfferItemBackup(helper, offer.Id, offer.Offeror, itemGuid));
                }
                foreach (Guid itemGuid in offer.OffereeItems)
                {
                    helper.Execute(OfferItemsQuery.Insert_OfferItemBackup(helper, offer.Id, offer.Offeree, itemGuid));
                }

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateOfferStatus(this SQLHelper helper, long id, OfferState status)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OffersQuery.Update_OfferStatus(helper, id, status));
                if (!helper.Success) throw new Exception($"更新报价 {id} 状态失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateOfferNegotiatedTimes(this SQLHelper helper, long id, int negotiatedTimes)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OffersQuery.Update_OfferNegotiatedTimes(helper, id, negotiatedTimes));
                if (!helper.Success) throw new Exception($"更新报价 {id} 协商次数失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateOfferFinishTime(this SQLHelper helper, long id, DateTime finishTime)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OffersQuery.Update_OfferFinishTime(helper, id, finishTime));
                if (!helper.Success) throw new Exception($"更新报价 {id} 完成时间失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteOffer(this SQLHelper helper, long id)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                // 删除 Offer 相关的 OfferItems
                helper.DeleteOfferItemsByOfferId(id);
                helper.Execute(OffersQuery.Delete_Offer(helper, id));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteOfferItemsByOfferId(this SQLHelper helper, long offerId)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OfferItemsQuery.Delete_OfferItemsByOfferId(helper, offerId));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteOfferItemsBackupByOfferId(this SQLHelper helper, long offerId)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OfferItemsQuery.Delete_OfferItemsBackupByOfferId(helper, offerId));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteOfferItem(this SQLHelper helper, long id)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(OfferItemsQuery.Delete_OfferItem(helper, id));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        private static void SetValue(SQLHelper helper, DataRow dr, Offer offer, bool isBackup)
        {
            offer.Id = (long)dr[OffersQuery.Column_Id];
            offer.Offeror = (long)dr[OffersQuery.Column_Offeror];
            offer.Offeree = (long)dr[OffersQuery.Column_Offeree];
            offer.Status = (OfferState)(int)dr[OffersQuery.Column_Status];
            offer.NegotiatedTimes = (int)dr[OffersQuery.Column_NegotiatedTimes];
            offer.CreateTime = (DateTime)dr[OffersQuery.Column_CreateTime];

            if (dr[OffersQuery.Column_FinishTime] != DBNull.Value && DateTime.TryParseExact(dr[OffersQuery.Column_FinishTime].ToString(), General.GeneralDateTimeFormat, null, System.Globalization.DateTimeStyles.None, out DateTime dt))
            {
                offer.FinishTime = dt;
            }

            // 获取 Offer 相关的 OfferItems
            if (isBackup)
            {
                offer.OfferorItems = [.. helper.GetOfferItemsBackupByOfferIdAndUserId(offer.Id, offer.Offeror)];
                offer.OffereeItems = [.. helper.GetOfferItemsBackupByOfferIdAndUserId(offer.Id, offer.Offeree)];
            }
            else
            {
                offer.OfferorItems = [.. helper.GetOfferItemsByOfferIdAndUserId(offer.Id, offer.Offeror)];
                offer.OffereeItems = [.. helper.GetOfferItemsByOfferIdAndUserId(offer.Id, offer.Offeree)];
            }
        }
    }
}
