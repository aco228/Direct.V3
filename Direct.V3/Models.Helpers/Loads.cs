using Direct.Models;
using Direct.Results;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Direct
{
  public static partial class DirectModelHelper
  {

    ///
    /// CREATE MODEL
    ///

    public static T CreateModel<T>(this DirectDatabaseBase db, int loadID) where T : DirectModel
    {
      T temp = (T)Activator.CreateInstance(typeof(T), db);
      temp.ID = loadID;
      temp.Snapshot.SetSnapshot();
      return temp;
    }

    ///
    /// LOAD BY ID (int)
    ///

    internal static string ContructLoadByID<T>(this DirectQueryLoader<T> loader, long id) where T : DirectModel
    {
      return string.Format("SELECT {0} FROM [].{1} WHERE {2}={3};",
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

    ///
    /// LOAD BY ID (string)
    ///

    internal static string ContructLoadByStringID<T>(this DirectQueryLoader<T> loader, string id) where T : DirectModel
    {
      return string.Format("SELECT {0} FROM [].{1} WHERE {2}='{3}';",
        loader.SelectQuery,
        loader.Instance.GetTableName(),
        loader.Instance.GetIdNameValue(), id);
    }
    
    public static T LoadByGuid<T>(this DirectQueryLoader<T> loader, string id) where T : DirectModel
    {
      var data = loader.Database.LoadSingle<T>(loader.ContructLoadByStringID(id));
      if (data != null)
        data.Database = loader.Database;
      return data;
    }

    ///
    /// LOAD BY WHERE
    ///

    internal static string ContructLoad<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      return string.Format("SELECT {0} FROM [].{1} {2} {3}",
        loader.SelectQuery,
        loader.Instance.TableName,
        loader.WhereQuery,
        loader.Additional);
    }

    public static T Load<T>(this DirectQueryLoader<T> loader, string query) where T : DirectModel
    {
      var result = loader.Database.LoadSingle<T>(query);
      if (result != null)
        result.Database = loader.Database;
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
