using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace IXMIgrationSample.Shared
{
    public class AccessDB
    { 
        public DataTable Read(String cmd, string conString)
        {
            SqlConnection con = new SqlConnection();
            //OracleConnection con = new OracleConnection();
            try
            {
                con.ConnectionString = conString;
                con.Open();
                DataTable tbl = new DataTable();
                SqlDataAdapter adapter = new SqlDataAdapter(cmd, con); 
                adapter.Fill(tbl);
                tbl.TableName = "tblStatus";
                return tbl;
            }
            catch (Exception ex)
            {
                 return null;
            }
            finally
            {
                con.Close();
            }
        }

        public DataTable ReadIX(String cmd, string conString)
        {
            OracleConnection con = new OracleConnection();
            try
            {
                con.ConnectionString = conString;
                con.Open();
                DataTable tbl = new DataTable();
                OracleDataAdapter adapter = new OracleDataAdapter(cmd, con);
                adapter.Fill(tbl);
                tbl.TableName = "tblStatus";
                return tbl;
            }
            catch (Exception ex)
            {
                return null;
            }
            finally
            {
                con.Close();
            }
        }

        public bool BulkInsertUpdate(List<string> queryList, string conString, string exceptionLogFile = @"C:\Ethiopian\ETFPR\AllExceptions.txt")
        {
            SqlConnection con = new SqlConnection();
            //OracleConnection con = new OracleConnection();
            bool status = true;
            try
            {
                con.ConnectionString = conString;

                string resultString = string.Join("\n ", queryList);

                con.Open();

                foreach (string query in queryList)
                {

                    SqlTransaction transaction = null;
                    try
                    {



                        //OracleTransaction transaction = null;
                        SqlCommand command = con.CreateCommand();

                        //OracleCommand command = con.CreateCommand();

                        transaction = con.BeginTransaction();
                        command.Transaction = transaction;

                        command.CommandText = query;
                        command.ExecuteNonQuery();

                        transaction.Commit();
                        status &= true;

                        //transaction.Dispose();
                        // con.Close();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        status &= false;

                     }
                }
                return status;
            }
            catch (Exception ex)
            {
                 return false;
            }
            finally
            {
                con.Close();
            }
        }

    }
}
