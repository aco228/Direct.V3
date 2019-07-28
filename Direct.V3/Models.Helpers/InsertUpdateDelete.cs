﻿using Direct.Results;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Direct.Models;

namespace Direct
{
  public static partial class DirectModelHelper
  {

    public static void InsertOrUpdate(this DirectDatabaseBase db, DirectModel model)
    {
      //if (model.LongID.HasValue)
      //  Update(db, model);
      //else
      //  Insert(db, model);
    }


    public static void InsertLater(this DirectModel model)
    {
      model.GetDatabase().TransactionalManager.Add(model.ConstructInsertQuery());
    }

    internal static string ConstructInsertQuery(this DirectModel model)
    {
      model.OnBeforeInsert();
      string command = string.Format("INSERT INTO {0}.{1}{2} ({3}) VALUES ({4});",
        model.GetDatabase().DatabaseName, model.GetDatabase().DatabaseSchemeString, model.GetTableName(),
        model.Snapshot.GetPropertyNamesForInsert(), model.Snapshot.GetPropertyValuesForInsert());
      return command;
    }

    public static T Insert<T>(this DirectDatabaseBase db, DirectModel model) where T : DirectModel
    {
      DirectExecuteResult result = db.Execute(model.ConstructInsertQuery());
      if (result.IsSuccessfull && result.LastID.HasValue)
      {
        model.ID = (int)result.LastID;
        model.Snapshot.SetSnapshot();
        return (T)model;
      }
      return (T)model;
    }


    internal static string ConstructUpdateQuery(this DirectModel model)
    {
      if (model.IntegerPrimary && !model.ID.HasValue)
        throw new Exception("ID is not set, maybe this table was not loaded");

      model.OnBeforeUpdate();

      // UPDATE MobilePaywall.core.A SET A=1 WHERE AID=1
      string command = string.Format("UPDATE {0}.{1}{2} SET {3} WHERE {4}={5};",
        model.GetDatabase().DatabaseName, model.GetDatabase().DatabaseSchemeString, model.GetTableName(),
        model.Snapshot.GetUpdateData(),
        model.GetIdNameValue(), 
        (model.IntegerPrimary ? model.ID.Value.ToString() : string.Format("'{0}'", model.GetStringID())));

      return command;
    }

    public static int? Update(this DirectDatabaseBase db, DirectModel model)
    {
      if (model.IntegerPrimary && !model.ID.HasValue)
        throw new Exception("ID is not set, maybe this table was not loaded");

      DirectExecuteResult result = db.Execute(model.ConstructUpdateQuery());
      if (!result.IsSuccessfull)
        return null;
      else
      {
        model.Snapshot.SetSnapshot();
        return result.NumberOfRowsAffected;
      }
    }

    

    public static bool Delete(this DirectDatabaseBase db, DirectModel model)
    {
      if (model.IntegerPrimary && !model.ID.HasValue)
        throw new Exception("THIS model has not ID");

      model.OnBeforeDelete();
      string command = string.Format("DELETE FROM {0}.{1}{2} WHERE {3}={4};",
        db.DatabaseName, db.DatabaseSchemeString, model.GetTableName(),
        model.GetIdNameValue(),
        (model.IntegerPrimary ? model.ID.Value.ToString() : string.Format("'{0}'", model.GetStringID())));
      DirectExecuteResult result = db.Execute(command);
      if (result.IsSuccessfull)
      {
        model.ID = null;
        model.Snapshot.DeleteSnapshot();
        return true;
      }
      return false;
    }

  }
}
