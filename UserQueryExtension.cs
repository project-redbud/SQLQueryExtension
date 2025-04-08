using System.Data;
using Milimoe.FunGame.Core.Api.Transmittal;
using Milimoe.FunGame.Core.Api.Utility;
using Milimoe.FunGame.Core.Entity;
using Milimoe.FunGame.Core.Library.SQLScript.Common;
using Milimoe.FunGame.Core.Library.SQLScript.Entity;

namespace ProjectRedbud.FunGame.SQLQueryExtension
{
    public static class UserQueryExtension
    {
        public static User? GetUserByUsernameAndPassword(this SQLHelper helper, string username, string password, bool loadInventory = false)
        {
            DataRow? dr = helper.ExecuteDataRow(UserQuery.Select_Users_LoginQuery(helper, username, password));
            if (dr != null)
            {
                User user = Factory.GetUser();
                SetValue(helper, dr, user, loadInventory);
                return user;
            }
            return null;
        }

        public static User? GetUserById(this SQLHelper helper, long id, bool loadInventory = false)
        {
            DataRow? dr = helper.ExecuteDataRow(UserQuery.Select_UserById(helper, id));
            if (dr != null)
            {
                User user = Factory.GetUser();
                SetValue(helper, dr, user, loadInventory);
                return user;
            }
            return null;
        }

        public static bool IsEmailExist(this SQLHelper helper, string email)
        {
            DataRow? dr = helper.ExecuteDataRow(UserQuery.Select_IsExistEmail(helper, email));
            return dr != null;
        }

        public static bool IsUsernameExist(this SQLHelper helper, string username, bool loadInventory = false)
        {
            DataRow? dr = helper.ExecuteDataRow(UserQuery.Select_IsExistUsername(helper, username));
            return dr != null;
        }

        public static User? GetUserByUsernameAndEmail(this SQLHelper helper, string username, string email)
        {
            DataRow? dr = helper.ExecuteDataRow(UserQuery.Select_CheckEmailWithUsername(helper, username, email));
            if (dr != null)
            {
                User user = Factory.GetUser();
                SetValue(helper, dr, user, false);
                return user;
            }
            return null;
        }

        public static List<User> GetUsersWhere(this SQLHelper helper, string where, Dictionary<string, object> args, bool loadInventory = false)
        {
            List<User> users = [];
            foreach (string key in args.Keys)
            {
                helper.Parameters[$"@{key.TrimStart(' ', '@', '?', ':')}"] = args[key];
            }
            helper.ExecuteDataSet(UserQuery.Select_Users_Where(helper, where));
            if (helper.Success)
            {
                foreach (DataRow dr in helper.DataSet.Tables[0].Rows)
                {
                    User user = Factory.GetUser();
                    SetValue(helper, dr, user, loadInventory);
                    users.Add(user);
                }
            }
            return users;
        }

        public static User? GetUserByUsernameAndAutoKey(this SQLHelper helper, string username, string autoKey, bool loadInventory = false)
        {
            DataRow? dr = helper.ExecuteDataRow(UserQuery.Select_CheckAutoKey(helper, username, autoKey));
            if (dr != null)
            {
                User user = Factory.GetUser();
                SetValue(helper, dr, user, loadInventory);
                return user;
            }
            return null;
        }

        public static void UpdateLastLogin(this SQLHelper helper, string username, string ipAddress)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(UserQuery.Update_CheckLogin(helper, username, ipAddress));
                if (!helper.Success) throw new Exception($"更新用户 {username} 的最后登录时间失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdatePassword(this SQLHelper helper, string username, string password)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(UserQuery.Update_Password(helper, username, password));
                if (!helper.Success) throw new Exception($"更新用户 {username} 的密码失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateGameTime(this SQLHelper helper, string username, int gameTimeMinutes)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(UserQuery.Update_GameTime(helper, username, gameTimeMinutes));
                if (!helper.Success) throw new Exception($"更新用户 {username} 的游戏时间失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void RegisterUser(this SQLHelper helper, string username, string password, string email, string ipAddress, string autoKey = "")
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(UserQuery.Insert_Register(helper, username, password, email, ipAddress, autoKey));
                if (!helper.Success) throw new Exception($"注册用户 {username} 失败。");

                long userId = helper.LastInsertId;
                helper.AddInventory(userId, "", 0, 0, 0);
                helper.Execute(UserSignIns.Insert_NewUserSignIn(helper, userId));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        private static void SetValue(SQLHelper helper, DataRow dr, User user, bool loadInventory)
        {
            user.Id = Convert.ToInt64(dr[UserQuery.Column_Id]);
            user.Username = dr[UserQuery.Column_Username].ToString() ?? "";
            user.RegTime = Convert.ToDateTime(dr[UserQuery.Column_RegTime]);
            user.LastTime = Convert.ToDateTime(dr[UserQuery.Column_LastTime]);
            user.Email = dr[UserQuery.Column_Email].ToString() ?? "";
            user.NickName = dr[UserQuery.Column_Nickname].ToString() ?? "";
            user.IsAdmin = Convert.ToInt32(dr[UserQuery.Column_IsAdmin]) != 0;
            user.IsOperator = Convert.ToInt32(dr[UserQuery.Column_IsOperator]) != 0;
            user.IsEnable = Convert.ToInt32(dr[UserQuery.Column_IsEnable]) != 0;
            user.GameTime = Convert.ToDouble(dr[UserQuery.Column_GameTime]);
            user.AutoKey = dr[UserQuery.Column_AutoKey].ToString() ?? "";
            if (loadInventory) helper.LoadInventory(user);
        }
    }
}
