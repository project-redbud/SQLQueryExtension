using System.Data;
using System.Text;
using Milimoe.FunGame.Core.Api.Transmittal;
using Milimoe.FunGame.Core.Api.Utility;
using Milimoe.FunGame.Core.Entity;
using Milimoe.FunGame.Core.Library.SQLScript.Entity;

namespace ProjectRedbud.FunGame.SQLQueryExtension
{
    public static class StoreQueryExtension
    {
        public static Store? GetStore(this SQLHelper helper, long storeId)
        {
            DataRow? dr = helper.ExecuteDataRow(StoreQuery.Select_StoreById(helper, storeId));
            if (dr != null)
            {
                Store store = new("");
                SetValue(dr, store);
                return store;
            }
            return null;
        }

        public static List<Store> GetStores(this SQLHelper helper, string where, Dictionary<string, object> args)
        {
            List<Store> stores = [];
            foreach (string key in args.Keys)
            {
                helper.Parameters[$"@{key.TrimStart(' ', '@', '?', ':')}"] = args[key];
            }
            helper.ExecuteDataSet(StoreQuery.Select_Stores + (where.Trim() != "" ? $" where {where}" : ""));
            if (helper.Success)
            {
                foreach (DataRow dr in helper.DataSet.Tables[0].Rows)
                {
                    Store store = new("");
                    SetValue(dr, store);
                    stores.Add(store);
                }
            }
            return stores;
        }

        public static List<Store> GetStoreWithGoods(this SQLHelper helper, params long[] storesId)
        {
            List<Store> stores = [];
            string where = "";
            if (storesId.Length > 0)
            {
                StringBuilder builder = new();
                builder.Append($" where {StoreQuery.Column_StoreId} in (");
                for (int i = 0; i < storesId.Length; i++)
                {
                    if (i > 0) builder.Append(", ");
                    builder.Append($"@id{storesId[i]}");
                    helper.Parameters[$"@id{storesId[i]}"] = storesId[i];
                }
                builder.Append(')');
                where = builder.ToString();
            }
            helper.ExecuteDataSet(StoreQuery.Select_StoresWithGoods + where);
            if (helper.Success)
            {
                foreach (DataRow dr in helper.DataSet.Tables[0].Rows)
                {
                    Store store = new((string)dr[StoreQuery.Column_StoreName])
                    {
                        Id = (long)dr[StoreQuery.Column_StoreId]
                    };
                    if (stores.FirstOrDefault(s => s.Id == store.Id) is Store temp)
                    {
                        store = temp;
                    }
                    else
                    {
                        stores.Add(store);
                    }

                    SetGoods(dr, store);
                }
            }
            return stores;
        }

        public static bool GetStoreGoods(this SQLHelper helper, Store store)
        {
            helper.ExecuteDataSet(StoreQuery.Select_AllGoodsInStore(helper, store.Id));
            if (helper.Success)
            {
                foreach (DataRow dr in helper.DataSet.Tables[0].Rows)
                {
                    SetGoods(dr, store);
                }
            }
            return helper.Success;
        }

        public static void AddStoreWithGoods(this SQLHelper helper, string storeName, DateTime? startTime, DateTime? endTime, IEnumerable<Goods> goods)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                // 新增商店
                helper.Execute(StoreQuery.Insert_Store(helper, storeName, startTime, endTime));
                if (!helper.Success) throw new Exception($"新增商店失败。");
                long storeId = helper.LastInsertId;

                foreach (Goods good in goods)
                {
                    // 更新或添加商品
                    if (helper.ExecuteDataRow(GoodsQuery.Select_GoodsById(helper, good.Id)) != null)
                    {
                        helper.Execute(GoodsQuery.Update_Goods(helper, good.Id, good.Name, good.Description, good.Stock));
                        if (!helper.Success) throw new Exception($"更新商品 {good.Name} 失败。");
                    }
                    else
                    {
                        helper.Execute(GoodsQuery.Insert_Goods(helper, good.Name, good.Description, good.Stock));
                        if (!helper.Success) throw new Exception($"新增商品 {good.Name} 失败。");
                        good.Id = helper.LastInsertId;
                    }

                    // 设置价格
                    helper.Execute(GoodsPricesQuery.Delete_GoodsPriceByGoodsId(helper, good.Id));
                    foreach (string currency in good.Prices.Keys)
                    {
                        double price = good.Prices[currency];
                        helper.Execute(GoodsPricesQuery.Insert_GoodsPrice(helper, good.Id, currency, price));
                        if (!helper.Success) throw new Exception($"设置商品 {good.Name} 的{currency}价格失败。");
                    }

                    // 添加物品
                    helper.Execute(GoodsItemsQuery.Delete_GoodsItemByGoodsId(helper, good.Id));
                    foreach (Item item in good.Items)
                    {
                        helper.Execute(GoodsItemsQuery.Insert_GoodsItem(helper, good.Id, item.Id));
                    }

                    // 将商品加入商店
                    if (helper.ExecuteDataRow(StoreGoodsQuery.Select_StoreGoodsByStoreIdAndGoodsId(helper, storeId, good.Id)) is null)
                    {
                        helper.Execute(StoreGoodsQuery.Insert_StoreGood(helper, storeId, good.Id));
                        if (!helper.Success) throw new Exception($"在商店 {storeName} 中新增商品 {good.Name} 失败。");
                    }
                }

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateGoods(this SQLHelper helper, IEnumerable<Goods> goods)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                foreach (Goods good in goods)
                {
                    // 更新或添加商品
                    if (helper.ExecuteDataRow(GoodsQuery.Select_GoodsById(helper, good.Id)) != null)
                    {
                        helper.Execute(GoodsQuery.Update_Goods(helper, good.Id, good.Name, good.Description, good.Stock));
                        if (!helper.Success) throw new Exception($"更新商品 {good.Name} 失败。");
                    }
                    else
                    {
                        helper.Execute(GoodsQuery.Insert_Goods(helper, good.Name, good.Description, good.Stock));
                        if (!helper.Success) throw new Exception($"新增商品 {good.Name} 失败。");
                        good.Id = helper.LastInsertId;
                    }

                    // 设置价格
                    helper.Execute(GoodsPricesQuery.Delete_GoodsPriceByGoodsId(helper, good.Id));
                    foreach (string currency in good.Prices.Keys)
                    {
                        double price = good.Prices[currency];
                        helper.Execute(GoodsPricesQuery.Insert_GoodsPrice(helper, good.Id, currency, price));
                        if (!helper.Success) throw new Exception($"设置商品 {good.Name} 的{currency}价格失败。");
                    }

                    // 添加物品
                    helper.Execute(GoodsItemsQuery.Delete_GoodsItemByGoodsId(helper, good.Id));
                    foreach (Item item in good.Items)
                    {
                        helper.Execute(GoodsItemsQuery.Insert_GoodsItem(helper, good.Id, item.Id));
                    }
                }

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateStore(this SQLHelper helper, Store store)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                if (helper.ExecuteDataRow(StoreQuery.Select_StoreById(helper, store.Id)) != null)
                {
                    // 更新商店
                    helper.Execute(StoreQuery.Update_Store(helper, store.Id, store.Name, store.StartTime, store.EndTime));
                    if (!helper.Success) throw new Exception($"更新商店 {store.Name} 失败。");
                }
                else
                {
                    // 新增商店
                    helper.Execute(StoreQuery.Insert_Store(helper, store.Name, store.StartTime, store.EndTime));
                    if (!helper.Success) throw new Exception($"新增商店 {store.Name} 失败。");
                    store.Id = helper.LastInsertId;
                }

                // 删除现有商品
                helper.Execute(StoreGoodsQuery.Delete_StoreGoodByStoreId(helper, store.Id));

                foreach (long goodsId in store.Goods.Keys)
                {
                    Goods good = store.Goods[goodsId];
                    if (helper.ExecuteDataRow(GoodsQuery.Select_GoodsById(helper, good.Id)) != null)
                    {
                        // 更新商品
                        helper.Execute(GoodsQuery.Update_Goods(helper, good.Id, good.Name, good.Description, good.Stock));
                        if (!helper.Success) throw new Exception($"更新商品 {good.Name} 失败。");
                    }
                    else
                    {
                        // 新增商品
                        helper.Execute(GoodsQuery.Insert_Goods(helper, good.Name, good.Description, good.Stock));
                        if (!helper.Success) throw new Exception($"新增商品 {good.Name} 失败。");
                        good.Id = helper.LastInsertId;
                    }

                    // 设置价格
                    helper.Execute(GoodsPricesQuery.Delete_GoodsPriceByGoodsId(helper, good.Id));
                    foreach (string currency in good.Prices.Keys)
                    {
                        double price = good.Prices[currency];
                        helper.Execute(GoodsPricesQuery.Insert_GoodsPrice(helper, good.Id, currency, price));
                        if (!helper.Success) throw new Exception($"设置商品 {good.Name} 的{currency}价格失败。");
                    }

                    // 添加物品
                    helper.Execute(GoodsItemsQuery.Delete_GoodsItemByGoodsId(helper, good.Id));
                    foreach (Item item in good.Items)
                    {
                        helper.Execute(GoodsItemsQuery.Insert_GoodsItem(helper, good.Id, item.Id));
                    }

                    // 将商品加入商店
                    if (helper.ExecuteDataRow(StoreGoodsQuery.Select_StoreGoodsByStoreIdAndGoodsId(helper, store.Id, good.Id)) is null)
                    {
                        helper.Execute(StoreGoodsQuery.Insert_StoreGood(helper, store.Id, good.Id));
                    }
                }

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteStoreWithGoods(this SQLHelper helper, long storeId)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                // 删除商店中的所有商品
                helper.Execute(StoreGoodsQuery.Delete_StoreGoodByStoreId(helper, storeId));

                // 删除商店
                helper.Execute(StoreQuery.Delete_Store(helper, storeId));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteStoreGoods(this SQLHelper helper, long storeId, long goodsId)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                // 从商店中删除商品
                helper.Execute(StoreGoodsQuery.Delete_StoreGoodByStoreIdAndGoodsId(helper, storeId, goodsId));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteGoods(this SQLHelper helper, long goodsId)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                // 删除商品的价格
                helper.Execute(GoodsPricesQuery.Delete_GoodsPriceByGoodsId(helper, goodsId));

                // 删除商品的物品
                helper.Execute(GoodsItemsQuery.Delete_GoodsItemByGoodsId(helper, goodsId));

                // 从所有商店中删除该商品
                helper.Execute(StoreGoodsQuery.Delete_StoreGoodByGoodsId(helper, goodsId));

                // 删除商品本身
                helper.Execute(GoodsQuery.Delete_Goods(helper, goodsId));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        private static void SetGoods(DataRow dr, Store store)
        {
            Goods goods = new()
            {
                Id = (long)dr[GoodsQuery.Column_Id]
            };
            if (store.Goods.TryGetValue(goods.Id, out Goods? value) && value != null)
            {
                goods = value;
            }
            goods.Name = (string)dr[GoodsQuery.Column_Name];
            goods.Description = (string)dr[GoodsQuery.Column_Description];
            goods.Stock = Convert.ToInt32(dr[GoodsQuery.Column_Stock]);
            string currency = (string)dr[GoodsPricesQuery.Column_Currency];
            double price = (double)dr[GoodsPricesQuery.Column_Price];
            goods.Prices[currency] = price;
            if (!goods.Items.Any(i => i.Id == (long)dr[GoodsItemsQuery.Column_ItemId]))
            {
                Item item = Factory.OpenFactory.GetInstance<Item>((long)dr[GoodsItemsQuery.Column_ItemId], "", []);
                goods.Items.Add(item);
            }
            store.Goods[goods.Id] = goods;
        }

        private static void SetValue(DataRow dr, Store store)
        {
            store.Id = (long)dr[StoreQuery.Column_Id];
            store.Name = (string)dr[StoreQuery.Column_StoreName];
            if (dr[StoreQuery.Column_StartTime] != DBNull.Value)
            {
                store.StartTime = (DateTime)dr[StoreQuery.Column_StartTime];
            }
            if (dr[StoreQuery.Column_EndTime] != DBNull.Value)
            {
                store.EndTime = (DateTime)dr[StoreQuery.Column_EndTime];
            }
        }
    }
}
