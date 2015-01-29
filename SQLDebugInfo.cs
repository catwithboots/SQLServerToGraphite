using System;
using System.Net.Sockets;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using Microsoft.SqlServer.Server;

namespace SQLServerToGraphite
{
    class SQLDebugInfo
    {
        public static string SendToSqlServer(string metrics)
        {
            /* We also like to return the information to SQL Server */
            SqlConnection conn = new SqlConnection("Context Connection=true");

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;

            string sqlcmd = "SELECT getdate() as [TimeStamp], '" + metrics.Length.ToString() + "' as [GraphiteStringLength], '" + metrics + "' as [GraphiteString]";
            cmd.CommandText = sqlcmd;

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            SqlContext.Pipe.Send(dr);

            dr.Close();
            conn.Close();

            return(metrics);
        }

        public static string SendMessageToSqlServer(string message)
        {
            /* We also like to return the information to SQL Server */
            SqlConnection conn = new SqlConnection("Context Connection=true");

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;

            string sqlcmd = "SELECT getdate() as [TimeStamp], '" + message + "' as [Message]";
            cmd.CommandText = sqlcmd;

            conn.Open();
            SqlDataReader dr = cmd.ExecuteReader();
            SqlContext.Pipe.Send(dr);

            dr.Close();
            conn.Close();

            return (message);
        }
    }
}
