using System.Data;
using Milimoe.FunGame.Core.Api.Transmittal;
using Milimoe.FunGame.Core.Api.Utility;
using Milimoe.FunGame.Core.Entity;
using Milimoe.FunGame.Core.Library.Constant;
using Milimoe.FunGame.Core.Library.SQLScript.Entity;

namespace ProjectRedbud.FunGame.SQLQueryExtension
{
    public static class RoomQueryExtension
    {
        public static Room? GetRoomByRoomid(this SQLHelper helper, string roomid)
        {
            DataRow? dr = helper.ExecuteDataRow(RoomQuery.Select_IsExistRoom(helper, roomid));
            if (dr != null)
            {
                Room room = Factory.GetRoom();
                SetValue(helper, dr, room);
                return room;
            }
            return null;
        }

        public static bool IsRoomExist(this SQLHelper helper, string roomid)
        {
            DataRow? dr = helper.ExecuteDataRow(RoomQuery.Select_IsExistRoom(helper, roomid));
            return dr != null;
        }

        public static List<Room> GetRooms(this SQLHelper helper)
        {
            List<Room> rooms = [];
            DataSet ds = helper.ExecuteDataSet(RoomQuery.Select_Rooms);
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    Room room = Factory.GetRoom();
                    SetValue(helper, dr, room);
                    rooms.Add(room);
                }
            }
            return rooms;
        }

        public static List<Room> GetRoomsByRoomState(this SQLHelper helper, params RoomState[] states)
        {
            List<Room> rooms = [];
            DataSet ds = helper.ExecuteDataSet(RoomQuery.Select_RoomsByRoomState(helper, states));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    Room room = Factory.GetRoom();
                    SetValue(helper, dr, room);
                    rooms.Add(room);
                }
            }
            return rooms;
        }

        public static List<Room> GetRoomsByGameModuleAndRoomState(this SQLHelper helper, string gameModule = "", params RoomState[] states)
        {
            List<Room> rooms = [];
            DataSet ds = helper.ExecuteDataSet(RoomQuery.Select_RoomsByGameModuleAndRoomState(helper, gameModule, states));
            if (helper.Success)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    Room room = Factory.GetRoom();
                    SetValue(helper, dr, room);
                    rooms.Add(room);
                }
            }
            return rooms;
        }

        public static void CreateRoom(this SQLHelper helper, string roomid, long roomMaster, RoomType roomType, string gameModule, string gameMap, bool isRank, string password, int maxUsers)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(RoomQuery.Insert_CreateRoom(helper, roomid, roomMaster, roomType, gameModule, gameMap, isRank, password, maxUsers));
                if (!helper.Success) throw new Exception($"创建房间 {roomid} 失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void UpdateRoomMaster(this SQLHelper helper, string roomid, long newRoomMaster)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(RoomQuery.Update_UpdateRoomMaster(helper, roomid, newRoomMaster));
                if (!helper.Success) throw new Exception($"更新房间 {roomid} 的房主失败。");

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void QuitRoomByRoomMaster(this SQLHelper helper, string roomid, long oldRoomMaster, long? newRoomMaster = null)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                if (newRoomMaster.HasValue)
                {
                    helper.Execute(RoomQuery.Update_QuitRoom(helper, roomid, oldRoomMaster, newRoomMaster.Value));
                    if (!helper.Success) throw new Exception($"更新房间 {roomid} 的房主失败。");
                }
                else
                {
                    helper.Execute(RoomQuery.Delete_QuitRoom(helper, roomid, oldRoomMaster));
                }

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        public static void DeleteRoom(this SQLHelper helper, string roomid)
        {
            bool hasTransaction = helper.Transaction != null;
            if (!hasTransaction) helper.NewTransaction();

            try
            {
                helper.Execute(RoomQuery.Delete_Rooms(helper, roomid));

                if (!hasTransaction) helper.Commit();
            }
            catch (Exception)
            {
                if (!hasTransaction) helper.Rollback();
                throw;
            }
        }

        private static void SetValue(SQLHelper helper, DataRow dr, Room room)
        {
            room.Id = Convert.ToInt64(dr[RoomQuery.Column_ID]);
            room.Roomid = dr[RoomQuery.Column_RoomID].ToString() ?? "-1";
            room.CreateTime = Convert.ToDateTime(dr[RoomQuery.Column_CreateTime]);
            long roomMasterId = Convert.ToInt64(dr[RoomQuery.Column_RoomMaster]);
            room.RoomMaster = helper.GetUserById(roomMasterId) ?? General.UnknownUserInstance;
            room.RoomType = (RoomType)Convert.ToInt32(dr[RoomQuery.Column_RoomType]);
            room.GameModule = dr[RoomQuery.Column_GameModule].ToString() ?? "";
            room.GameMap = dr[RoomQuery.Column_GameMap].ToString() ?? "";
            room.RoomState = (RoomState)Convert.ToInt32(dr[RoomQuery.Column_RoomState]);
            room.IsRank = Convert.ToInt32(dr[RoomQuery.Column_IsRank]) != 0;
            room.Password = dr[RoomQuery.Column_Password].ToString() ?? "";
            room.MaxUsers = Convert.ToInt32(dr[RoomQuery.Column_MaxUsers]);
            room.Statistics = new GameStatistics(room);
        }
    }
}
