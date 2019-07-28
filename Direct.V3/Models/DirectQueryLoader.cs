using Direct.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Direct
{
  public class DirectQueryLoader<T> where T : DirectModel
  {
    private T _instance = null;
    private List<string> constructWhereParameters = null;

    public DirectDatabaseBase Database { get; set; }
    public string Select { get; set; } = string.Empty;
    public string Where { get; set; } = string.Empty;
    public string Additional { get; set; } = string.Empty;

    public void SetWhere(string where, params object[] parameters)
    {
      this.Where = this.Database.Construct(where, parameters);
    }

    public void AddWhere(string pattern, params object[] parameters)
    {
      if (this.constructWhereParameters == null)
        this.constructWhereParameters = new List<string>();

      this.constructWhereParameters.Add(this.Database.Construct(pattern, parameters));
    }
    public string WhereQuery
    {
      get
      {
        string where = this.Where;
        if (this.constructWhereParameters != null)
        {
          foreach (string q in this.constructWhereParameters)
            where += (!string.IsNullOrEmpty(where) ? " AND " : "") + q;

          this.constructWhereParameters = null;
        }

        return string.IsNullOrEmpty(where) ? "" : " WHERE " + where;
      }
    }


    public string SelectQuery { get => string.IsNullOrEmpty(this.Select) ? "*" : this.Select; }
    public T Instance
    {
      get
      {
        if (this._instance != null)
          return this._instance;
        this._instance = (T)Activator.CreateInstance(typeof(T), this.Database);
        return this._instance;
      }
    }

  }
}
