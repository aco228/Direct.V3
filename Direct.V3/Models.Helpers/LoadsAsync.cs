using Direct.Models;
using Direct.Results;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Direct
{
  public static partial class DirectModelHelper
  {

    public static async Task<T> LoadAsync<T>(this DirectQueryLoader<T> loader, int id) where T : DirectModel
    {
      string command = string.Format("SELECT {0} FROM [].{1} WHERE {2}={3};",
        loader.SelectQuery,
        loader.Instance.GetTableName(),
        loader.Instance.GetIdNameValue(), id);

      var data = await loader.Database.LoadSingleAsync<T>(command);
      if(data != null)
      {
        data.Snapshot.SetSnapshot();
        data.Database = loader.Database;
      }
      return data;
    }

    public static async Task<T> LoadByGuidAsync<T>(this DirectQueryLoader<T> loader, string id) where T : DirectModel
    {
      string command = string.Format("SELECT {0} FROM [].{1} WHERE {2}='{3}';",
        loader.SelectQuery,
        loader.Instance.GetTableName(),
        loader.Instance.GetIdNameValue(), id);

      var data = await loader.Database.LoadSingleAsync<T>(command);
      if(data != null)
      {
        data.Snapshot.SetSnapshot();
        data.Database = loader.Database;
      }
      return data;
    }

    public static async Task<T> LoadAsync<T>(this DirectQueryLoader<T> loader, string query) where T : DirectModel
    {
      var data = await loader.Database.LoadSingleAsync<T>(query); ;
      if (data != null)
      {
        data.Snapshot.SetSnapshot();
        data.Database = loader.Database;
      }
      return data;
    }

    public static async Task<List<T>> LoadAsync<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      string command = string.Format("SELECT {0} FROM [].{1} {2} {3}",
        loader.SelectQuery,
        loader.Instance.TableName,
        loader.WhereQuery,
        loader.Additional);

      List<T> result = new List<T>();
      foreach (var row in await loader.Database.LoadAsync<T>(command))
      {
        row.Snapshot.SetSnapshot();
        row.Database = loader.Database;
        result.Add(row);
      }

      return result;
    }

    public static async Task<T> LoadSingleAsync<T>(this DirectQueryLoader<T> loader) where T : DirectModel
    {
      string command = string.Format("SELECT {0} FROM [].{1} {2} LIMIT 1",
        loader.SelectQuery,
        loader.Instance.TableName,
        loader.WhereQuery);

      var data = await loader.Database.LoadSingleAsync<T>(command);
      if(data != null)
      {
        data.Snapshot.SetSnapshot();
        data.Database = loader.Database;
      }
      return data;
    }

  }
}
