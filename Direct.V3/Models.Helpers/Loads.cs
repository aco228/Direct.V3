using Direct.Models;
using Direct.Results;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Direct
{
  public static partial class DirectModelHelper
  {

    //
    // CREATE MODEL
    //

    /// <summary>
    /// Create single empty instance (dummy object) of selected type, with predefined ID
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="db"></param>
    /// <param name="loadID">ID</param>
    /// <returns></returns>
    public static T CreateModel<T>(this DirectDatabaseBase db, int loadID) where T : DirectModel
    {
      T temp = (T)Activator.CreateInstance(typeof(T), db);
      temp.ID = loadID;
      temp.Snapshot.SetSnapshot();
      return temp;
    }

    /// <summary>
    /// Create single empty instance (dummy object) of selected type, with predefined ID
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="db"></param>
    /// <param name="loadID">ID</param>
    /// <returns></returns>
    public static T CreateModel<T>(this DirectDatabaseBase db, string loadID) where T : DirectModel
    {
      T temp = (T)Activator.CreateInstance(typeof(T), db);
      temp.SetStringID(loadID);
      temp.Snapshot.SetSnapshot();
      return temp;
    }

    //
    // LOAD BY ID (int)
    //

    internal static string ContructLoadByID<T>(this DirectQueryLoader<T> loader, long id) where T : DirectModel
    {
      return string.Format(loader.Database.QueryContructLoadByID,
        loader.SelectQuery,
        loader.Instance.GetTableName(),
        loader.Instance.GetIdNameValue(), id);
    }

    public static T Load<T>(this DirectQueryLoader<T> loader, int id) where T : DirectModel
      => Load(loader, (long)id);

    public static T Load<T>(this DirectQueryLoader<T> loader, long id) where T : DirectModel
    {
      var data = loader.Database.LoadSingle<T>(loader.ContructLoadByID(id));
      if (data != null)
        data.Database = loader.Database;
      return data;
    }

    public static T LoadSingle<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      string command = string.Format(loader.Database.QueryLoadSingle,
        loader.SelectQuery,
        loader.Instance.TableName,
        loader.WhereQuery);

      var data = loader.Database.LoadSingle<T>(command);
      if (data != null)
      {
        data.Snapshot.SetSnapshot();
        data.Database = loader.Database;
      }
      return data;
    }

    //
    // LOAD BY ID (string)
    //

    internal static string ContructLoadByStringID<T>(this DirectQueryLoader<T> loader, string id) where T : DirectModel
    {
      return string.Format(loader.Database.QueryContructLoadByStringID,
        loader.SelectQuery,
        loader.Instance.GetTableName(),
        loader.Instance.GetIdNameValue(), id);
    }

    public static T LoadByGuid<T>(this DirectQueryLoader<T> loader, string id) where T : DirectModel
    {
      var data = loader.Database.LoadSingle<T>(loader.ContructLoadByStringID(id));
      if (data != null)
      {
        data.Snapshot.SetSnapshot();
        data.Database = loader.Database;
      }
      return data;
    }

    //
    // LOAD BY WHERE
    //

    internal static string ContructLoad<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      return string.Format(loader.Database.QueryContructLoad,
        loader.SelectQuery,
        loader.Instance.TableName,
        loader.WhereQuery,
        loader.Additional);
    }

    public static T Load<T>(this DirectQueryLoader<T> loader, string query) where T : DirectModel
    {
      var result = loader.Database.LoadSingle<T>(query);
      if (result != null)
      {
        result.Snapshot.SetSnapshot();
        result.Database = loader.Database;
      }
      return result;
    }

    public static IEnumerable<T> LoadEnumerable<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      foreach (var row in loader.Database.Loader.LoadEnumerable<T>(loader.ContructLoad()))
      {
        row.Database = loader.Database;
        yield return row;
      }
      yield break;
    }

    public static List<T> Load<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      List<T> result = new List<T>();
      foreach (var entry in LoadEnumerable<T>(loader))
      {
        entry.Snapshot.SetSnapshot();
        entry.Database = loader.Database;
        result.Add(entry);
      }
      return result;
    }

    public static T LoadLater<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      using (var tempValue = (T)Activator.CreateInstance(typeof(T), (DirectDatabaseBase)null))
        loader.Select = tempValue.IdName;

      T temp = (T)Activator.CreateInstance(typeof(T), loader.Database);
      loader.Database.TransactionalManager.Load(temp, loader.ContructLoad());
      return temp;
    }


  }
}
