using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.SqlServer.Server;

namespace SQLServerToGraphite
{
    public class StoredProcedures
    {
        [SqlProcedure()]
        public static void GraphiteData(string server, Int32 port, string path, decimal value, Int32 debug)
        {
            /* Calculate the unix epoch time */
            TimeSpan t = DateTime.UtcNow - new DateTime(1970, 1, 1);
            int epochtime = (int)t.TotalSeconds;

            /* Test if the provided IP or host is valid */
            IPAddress address = Checks.ParseAddress(server);

            /* Open a TCP socket to the "server/port" and open stream */
            TcpClient client = new TcpClient();
            client.Connect(server, port);
            NetworkStream netStream = client.GetStream();

            /* Build the graphite string we want to send */
            string metrics = path.ToString() + " " + value.ToString().Replace(",", ".") + " " + epochtime.ToString() + "\n";

            /* Test if we can actually write stuff to the network stream*/
            if (netStream.CanWrite)
            {
                Byte[] sendBytes = Encoding.UTF8.GetBytes(metrics);
                netStream.Write(sendBytes, 0, sendBytes.Length);
            }
            else
            {
                //Return message with a write issue in Graphite
                string message = "Could not write data to Graphite: " + server + " connected=" + client.Connected.ToString() + " readtimeout= " + netStream.ReadTimeout.ToString() + " CanWrite= " + netStream.CanWrite.ToString();

                // Closing the tcpClient instance does not close the network stream.
                netStream.Close();
                client.Close();

                // Also send metric info back to SQL if debug is set to 1 before exiting
                if (debug == 1)
                {
                    SQLDebugInfo.SendMessageToSqlServer(message);
                }

                // Exit the CLR
                return;
            }

            netStream.Close();
            client.Close();

            /* SQL Server part (only with debug bit set to 1) */
            if (debug == 1)
            {
                SQLDebugInfo.SendToSqlServer(metrics);
            }
        }

        [SqlProcedure()]
        public static void GraphiteDataBulk(String server, Int32 port, String bulkmetrics, Int32 debug)
        {
            /* Calculate the unix epoch time */
            TimeSpan t = DateTime.UtcNow - new DateTime(1970, 1, 1);
            int epochtime = (int)t.TotalSeconds;

            /* Test if the provided IP or host is valid */
            IPAddress address = Checks.ParseAddress(server);

            /* Open a TCP socket to the "server/port" and open stream */
            TcpClient client = new TcpClient();
            client.Connect(server, port);
            client.SendTimeout = 5000;
            NetworkStream netStream = client.GetStream();
            netStream.ReadTimeout = 5000;

            /* Build the graphite string we want to send */
            string metrics = bulkmetrics.ToString().Replace(",", " " + epochtime.ToString() + "\n") + " " + epochtime.ToString() + "\n";

            /* Test if we can actually write stuff to the network stream*/
            if (netStream.CanWrite)
            {
                Byte[] sendBytes = Encoding.UTF8.GetBytes(metrics);
                netStream.Write(sendBytes, 0, sendBytes.Length);
            }
            else
            {
                //Return message with a write issue in Graphite
                string message = "Could not write data to Graphite: " + server + " connected=" + client.Connected.ToString() + " readtimeout= " + netStream.ReadTimeout.ToString() + " CanWrite= " + netStream.CanWrite.ToString();

                // Closing the tcpClient instance does not close the network stream.
                netStream.Close();
                client.Close();

                // Also send metric info back to SQL if debug is set to 1 before exiting
                if (debug == 1)
                {
                    SQLDebugInfo.SendMessageToSqlServer(message);
                }

                // Exit the CLR
                return;
            }

            netStream.Close();
            client.Close();

            /* SQL Server part (only with debug bit set to 1) */

            if (debug == 1)
            {
                /* SQL Server part (only with debug bit set to 1) */
                SQLDebugInfo.SendToSqlServer(metrics);
            }
        }

    }
}
