using System.Data;
using Milimoe.FunGame.Core.Api.Transmittal;
using Milimoe.FunGame.Core.Api.Utility;
using Milimoe.FunGame.Core.Entity;
using Milimoe.FunGame.Core.Library.Constant;
using Milimoe.FunGame.Core.Library.SQLScript.Entity;

namespace ProjectRedbud.FunGame.SQLQueryExtension
{
    public static class InventoryQueryExtension
    {
        public static bool LoadInventory(this SQLHelper helper, User user, bool useCharacterFactory = false)
        {
            DataRow? dr = helper.ExecuteDataRow(InventoriesQuery.Select_InventoryByUserId(helper, user.Id));
            if (dr != null)
            {
                user.Inventory.Name = dr[InventoriesQuery.Column_Name].ToString() ?? "";
                user.Inventory.Credits = Convert.ToDouble(dr[InventoriesQuery.Column_Credits]);
                user.Inventory.Materials = Convert.ToDouble(dr[InventoriesQuery.Column_Materials]);
                long mainCharacter = Convert.ToInt64(dr[InventoriesQuery.Column_MainCharacter]);
                LoadUserCharacters(helper, user, mainCharacter, useCharacterFactory);
                LoadUserItems(helper, user);
            }
            return dr != null;
        }

        public static void AddInventory(this SQLHelper helper, long userId, string name, double credits, double materials, long mainCharacter)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(InventoriesQuery.Insert_Inventory(helper, userId, name, credits, materials, mainCharacter));
                if (!helper.Success) throw new Exception($"新增用户 {userId} 的库存失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateInventory(this SQLHelper helper, long userId, string name, double credits, double materials, long mainCharacter)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(InventoriesQuery.Update_Inventory(helper, userId, name, credits, materials, mainCharacter));
                if (!helper.Success) throw new Exception($"更新用户 {userId} 的库存失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }
        
        public static void UpdateInventory(this SQLHelper helper, Inventory inventory)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                User user = inventory.User;
                helper.Execute(InventoriesQuery.Update_Inventory(helper, user.Id, inventory.Name, inventory.Credits, inventory.Materials, inventory.MainCharacter.Id));
                if (!helper.Success) throw new Exception($"更新用户 {user.Id} 的库存失败。");

                helper.Execute(UserCharactersQuery.Delete_UserCharactersByUserId(helper, user.Id));
                foreach (Character character in inventory.Characters)
                {
                    helper.Execute(UserCharactersQuery.Insert_UserCharacter(helper, character.Id, user.Id, character.Name, character.FirstName, character.NickName,
                        character.PrimaryAttribute, character.InitialATK, character.InitialDEF, character.InitialHP, character.InitialMP, character.InitialSTR, character.InitialAGI,
                        character.InitialINT, character.InitialSPD, character.InitialHR, character.InitialMR, character.Level, character.LevelBreak, true, null));
                    if (!helper.Success) throw new Exception($"更新用户 {user.Id} 的角色 {character.Id} 失败。");
                }
                
                helper.Execute(UserItemsQuery.Delete_UserItemsByUserId(helper, user.Id));
                foreach (Item item in inventory.Items)
                {
                    helper.Execute(UserItemsQuery.Insert_UserItem(helper, item.Id, user.Id, item.Character != null ? item.Character.Id : 0, item.Name, item.IsLock, item.Equipable,
                        item.Unequipable, item.EquipSlotType, item.Key, item.Enable, item.Price, item.IsSellable, item.IsTradable, item.NextSellableTime, item.NextTradableTime, item.RemainUseTimes));
                    if (!helper.Success) throw new Exception($"更新用户 {user.Id} 的物品 {item.Id} 失败。");
                }

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateInventoryCredits(this SQLHelper helper, long userId, double credits)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(InventoriesQuery.Update_InventoryCredits(helper, userId, credits));
                if (!helper.Success) throw new Exception($"更新用户 {userId} 的 Credits 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateInventoryMaterials(this SQLHelper helper, long userId, double materials)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(InventoriesQuery.Update_InventoryMaterials(helper, userId, materials));
                if (!helper.Success) throw new Exception($"更新用户 {userId} 的 Materials 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateInventoryMainCharacter(this SQLHelper helper, long userId, long mainCharacter)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(InventoriesQuery.Update_InventoryMainCharacter(helper, userId, mainCharacter));
                if (!helper.Success) throw new Exception($"更新用户 {userId} 的 MainCharacter 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteInventory(this SQLHelper helper, long userId)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(InventoriesQuery.Delete_Inventory(helper, userId));
                if (!helper.Success) throw new Exception($"删除用户 {userId} 的库存失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        private static void LoadUserItems(SQLHelper helper, User user)
        {
            DataSet ds = helper.ExecuteDataSet(UserItemsQuery.Select_UserItemsByUserId(helper, user.Id));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    Item item = Factory.OpenFactory.GetInstance<Item>((long)dr[UserItemsQuery.Column_ItemId], "", []);
                    item.User = user;
                    item.Name = dr[UserItemsQuery.Column_ItemName].ToString() ?? "";
                    item.IsLock = Convert.ToInt32(dr[UserItemsQuery.Column_IsLock]) != 0;
                    item.Equipable = Convert.ToInt32(dr[UserItemsQuery.Column_Equipable]) != 0;
                    item.Unequipable = Convert.ToInt32(dr[UserItemsQuery.Column_Unequipable]) != 0;
                    item.EquipSlotType = (EquipSlotType)Convert.ToInt32(dr[UserItemsQuery.Column_EquipSlotType]);
                    item.Key = Convert.ToInt32(dr[UserItemsQuery.Column_Key]);
                    item.Enable = Convert.ToInt32(dr[UserItemsQuery.Column_Enable]) != 0;
                    item.Price = Convert.ToDouble(dr[UserItemsQuery.Column_Price]);
                    item.IsSellable = Convert.ToInt32(dr[UserItemsQuery.Column_IsSellable]) != 0;
                    item.IsTradable = Convert.ToInt32(dr[UserItemsQuery.Column_IsTradable]) != 0;
                    if (dr[UserItemsQuery.Column_NextSellableTime] != DBNull.Value && DateTime.TryParseExact(dr[UserItemsQuery.Column_NextSellableTime].ToString(), General.GeneralDateTimeFormat, null, System.Globalization.DateTimeStyles.None, out DateTime dt))
                    {
                        item.NextSellableTime = dt;
                    }
                    if (dr[UserItemsQuery.Column_NextTradableTime] != DBNull.Value && DateTime.TryParseExact(dr[UserItemsQuery.Column_NextTradableTime].ToString(), General.GeneralDateTimeFormat, null, System.Globalization.DateTimeStyles.None, out dt))
                    {
                        item.NextTradableTime = dt;
                    }
                    item.RemainUseTimes = Convert.ToInt32(dr[UserItemsQuery.Column_RemainUseTimes]);
                    if (user.Inventory.Characters.FirstOrDefault(c => c.Id == Convert.ToInt64(dr[UserItemsQuery.Column_CharacterId])) is Character character)
                    {
                        character.Equip(item, item.EquipSlotType, out _);
                    }
                }
            }
        }

        private static void LoadUserCharacters(SQLHelper helper, User user, long mainCharacter, bool useFactory = false)
        {
            DataSet ds = helper.ExecuteDataSet(UserCharactersQuery.Select_UserCharactersByUserId(helper, user.Id));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    Character character;
                    if (useFactory)
                    {
                        character = Factory.OpenFactory.GetInstance<Character>((long)dr[UserCharactersQuery.Column_CharacterId], "", []);
                    }
                    else
                    {
                        character = Factory.GetCharacter();
                        character.Id = (long)dr[UserCharactersQuery.Column_CharacterId];
                        character.User = user;
                        character.Name = dr[UserCharactersQuery.Column_Name].ToString() ?? "";
                        character.FirstName = dr[UserCharactersQuery.Column_FirstName].ToString() ?? "";
                        character.NickName = dr[UserCharactersQuery.Column_NickName].ToString() ?? "";
                        character.PrimaryAttribute = (PrimaryAttribute)Convert.ToInt32(dr[UserCharactersQuery.Column_PrimaryAttribute]);
                        character.InitialATK = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialATK]);
                        character.InitialDEF = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialDEF]);
                        character.InitialHP = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialHP]);
                        character.InitialMP = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialMP]);
                        character.InitialAGI = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialAGI]);
                        character.InitialINT = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialINT]);
                        character.InitialSTR = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialSTR]);
                        character.InitialSPD = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialSPD]);
                        character.InitialHR = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialHR]);
                        character.InitialMR = Convert.ToDouble(dr[UserCharactersQuery.Column_InitialMR]);
                    }
                    if (dr[UserCharactersQuery.Column_InSquad].Equals(true))
                    {
                        user.Inventory.Squad.Add(character.Id);
                    }
                    if (dr[UserCharactersQuery.Column_TrainingTime] != DBNull.Value)
                    {
                        user.Inventory.Training.Add(character.Id, Convert.ToDateTime(dr[UserCharactersQuery.Column_TrainingTime]));
                    }
                    if (character.Id != 0 && mainCharacter != 0 && character.Id == mainCharacter)
                    {
                        user.Inventory.MainCharacter = character;
                    }
                }
            }
        }
    }
}
