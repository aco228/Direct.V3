using Direct.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Direct.Models
{
  public abstract class DirectModel : IDisposable
  {
    internal string InternalID { get; private set; } = string.Empty;
    internal DirectModelSnapshot Snapshot { get; set; } = null;
    internal string IdName { get; set; } = string.Empty;
    internal string TableName { get; set; } = string.Empty;
    internal string BulkVariableName => string.Format("{0}_{1}", this.IdName, this.InternalID);
    internal DirectDatabaseBase Database { get; set; } = null;
    internal bool IntegerPrimary { get; set; } = true;


    public int? ID
    {
      get
      {
        if (!this.IntegerPrimary) return null;
        int? result = (int?)this.Snapshot.IdPropertyInfo.GetValue(this);
        return result.HasValue && result.Value == 0 ? null : result;
      }
      set
      {
        if (!this.IntegerPrimary) return;
        this.Snapshot.IdPropertyInfo.SetValue(this, value);
      }
    }
    public string GetStringID()
    {
      if (this.IntegerPrimary) return string.Empty;
      string result = (string)this.Snapshot.IdPropertyInfo.GetValue(this);
      if (!string.IsNullOrEmpty(result))
        return result;
      return this.InternalID;
    }
    public string SetStringID(string id)
    {
      if (this.IntegerPrimary) return string.Empty;
      this.Snapshot.IdPropertyInfo.SetValue(this, id);
      this.InternalID = id;
      return id;
    }
    public DirectDatabaseBase GetDatabase() => this.Database;

    ///
    /// CONSTRUCTOR && DECONSTRUCTOR
    ///

    public DirectModel(string tableName, string id_name, DirectDatabaseBase db)
    {
      this.TableName = tableName;
      this.IdName = id_name;
      this.Database = db;
      this.InternalID = this.ConstructSignature() + Guid.NewGuid().ToString().Replace("-", string.Empty);

      this.Snapshot = new DirectModelSnapshot(this);
      this.PrepareProperties();
      this.Snapshot.SetSnapshot();
    }

    ~DirectModel() => OnDispose();
    public void Dispose() => OnDispose();

    protected void OnDispose()
    {
      if (this.Database != null)
        this.Database.Dispose();
    }

    private string ConstructSignature()
    {
      string[] split = (from s in this.TableName.Split('_') where s.Length > 2 select s).ToArray();
      if (split.Length == 1)
        return split[0].Substring(0, 3).ToUpper();
      else 
        return split[0].Substring(0, 1).ToUpper() + split[1].Substring(0, 2).ToUpper();
    }

    ///
    /// Get data
    ///

    internal string GetTableName() => this.TableName;
    internal string GetIdNameValue() => this.IdName;

    public int? WaitID(int forSeconds = 30)
    {
      if (this.ID.HasValue)
        return this.ID;

      DateTime started = DateTime.Now;
      do
      {
        if ((DateTime.Now - started).TotalSeconds >= forSeconds)
          break;
      }
      while (this.ID == null);

      return this.ID;
    }

    public int WaitIDExplicit(int forSeconds = 30)
    {
      int? id = this.WaitID(30);
      if (!id.HasValue)
        throw new Exception("We did not get any response");
      return id.Value;
    }

    ///
    /// Overrides
    ///

    internal Action OnAfterInsert = null;
    internal Action OnAfterUpdate = null;

    public void SetOnAfterInsert(Action action) => this.OnAfterInsert = action;
    public void SetOnAfterUpdate(Action action) => this.OnAfterUpdate = action;

    public virtual void OnBeforeInsert() { }
    public virtual void OnBeforeUpdate() { }
    public virtual void OnBeforeDelete() { }


    //
    // SUMMARY: Properties manipulation
    internal void PrepareProperties()
      => this.Snapshot.PrepareProperties();

    internal DirectDatabaseBase GetDatabase(DirectDatabaseBase db = null)
    {
      if (db != null) return db;
      if (this.Database != null) return this.Database;
      throw new Exception("Database is not set!!");
    }


    ///
    /// LOAD
    /// 


    ///
    /// INSERT
    /// 

    public void Insert(DirectDatabaseBase db = null) => this.GetDatabase(db).Insert<DirectModel>(this);
    public T Insert<T>(DirectDatabaseBase db = null) where T : DirectModel => this.GetDatabase(db).Insert<T>(this);

    public Task InsertAsync(DirectDatabaseBase db = null) => this.GetDatabase(db).InsertAsync<DirectModel>(this);
    public Task<T> InsertAsync<T>(DirectDatabaseBase db = null) where T : DirectModel => this.GetDatabase(db).InsertAsync<T>(this);

    public void InsertLater(DirectDatabaseBase db = null)
    {
      this.OnAfterInsert?.Invoke();
      this.GetDatabase(db).TransactionalManager.Insert(this);
    }
    public void InsertOrUpdate(DirectDatabaseBase db = null) => this.GetDatabase(db).InsertOrUpdate(this);
    public async Task InsertOrUpdateAsync(DirectDatabaseBase db = null) => await this.GetDatabase(db).InsertOrUpdateAsync(this);


    /// 
    /// UPDATE
    /// 

    public void Update(DirectDatabaseBase db = null) => this.GetDatabase(db).Update(this);
    public void UpdateLater()
    {
      this.OnAfterUpdate?.Invoke();
      this.GetDatabase().TransactionalManager.Add(this);
    }
    public async Task UpdateAsync(DirectDatabaseBase db = null) => await this.GetDatabase(db).UpdateAsync(this);

    /// 
    /// DELETE
    /// 

    public bool Delete(DirectDatabaseBase db = null) => this.GetDatabase(db).Delete(this);




  }
}
