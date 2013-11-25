using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Nats
{
    sealed class Subscription
    {
        public int Id { get; private set; }

        public string Topic { get; private set; }

        public Action<string, string> Handler { get; private set; }

        public Subscription(int id, string topic, Action<string, string> handler)
        {
            Id = id;
            Topic = topic;
            Handler = handler;
        }
    }

    sealed class Message
    {
        public int Sid { get; private set; }

        public string Topic { get; private set; }

        public string Reply { get; private set; }

        public int BodyLen { get; private set; }

        public string Body { get; set; }

        public Message(int sid, string topic, string reply, int bodyLen)
        {
            Sid = sid;
            Topic = topic;
            Reply = reply;
            BodyLen = bodyLen;
        }
    }

    sealed class Connection : IDisposable
    {
        private TcpClient client;

        public NetworkStream Stream { get; private set; }

        public StreamReader Reader { get; private set; }

        public Connection(string host, int port)
        {
            client = new TcpClient(host, port);
            client.NoDelay = true;
            Stream = client.GetStream();
            Reader = new StreamReader(Stream);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Send(string[] lines)
        {
            Send(string.Join("\r\n", lines));
        }

        public void Send(string command)
        {
            Debug.WriteLine(command, "NATS-CLIENT-SEND");
            var bytes = Encoding.ASCII.GetBytes(command + "\r\n");
            Stream.Write(bytes, 0, bytes.Length);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (Reader != null)
                {
                    Reader.Close();
                }
                if (client != null)
                {
                    client.Close();
                }
            }
            Reader = null;
            Stream = null;
            client = null;
        }

        ~Connection()
        {
            Dispose(false);
        }
    }

    public sealed class NatsClient : IDisposable
    {
        private string serverAddr = "localhost";
        private int    serverPort = 4222;
        private string connectMsg;

        private bool disposed = false;
        private CancellationTokenSource cancellation = new CancellationTokenSource();

        private int sidBase = 0;
        private IDictionary<long, Subscription> subscriptions = new Dictionary<long, Subscription>();
        private Connection connection;
        private Task processor;
        private object stateLock = new object();

        public NatsClient(Uri natsUrl)
        {
            BuildConnectMsg(natsUrl);
        }

        ~NatsClient()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Connect()
        {
            ThrowIfDisposed();
            SyncedIf(() => processor == null, () => processor = Task.Factory.StartNew(() => Processing()));
        }

        public int Subscribe(string topic, Action<string, string> messageHandler)
        {
            ThrowIfDisposed();
            var sub = new Subscription(Interlocked.Increment(ref sidBase), topic, messageHandler);
            lock (stateLock)
            {
                subscriptions.Add(sub.Id, sub);
                if (connection != null)
                {
                    SendSubscribe(connection, sub);
                }
            }
            return sub.Id;
        }

        public void Unsubscribe(int sid)
        {
            ThrowIfDisposed();
            lock (stateLock)
            {
                if (subscriptions.Remove(sid) && connection != null)
                {
                    SendUnsubscribe(connection, sid);
                }
            }
        }

        public bool Publish(string topic, string message)
        {
            ThrowIfDisposed();
            lock (stateLock)
            {
                return WhenConnected((conn) => SendPublish(conn, topic, message));
            }
        }

        private void BuildConnectMsg(Uri natsUrl)
        {
            string user = null, pass = null;
            if (natsUrl != null)
            {
                serverAddr = natsUrl.Host;
                if (natsUrl.Port > 0)
                {
                    serverPort = natsUrl.Port;
                }

                if (natsUrl.UserInfo.Length > 0)
                {
                    var pos = natsUrl.UserInfo.IndexOf(':');
                    if (pos >= 0)
                    {
                        user = natsUrl.UserInfo.Substring(0, pos);
                        pass = natsUrl.UserInfo.Substring(pos + 1);
                    }
                    else
                    {
                        user = natsUrl.UserInfo;
                    }
                }
            }

            connectMsg = "CONNECT { \"verbose\":false, \"pedantic\":false ";
            if (user != null)
            {
                connectMsg += "\"user\":\"" + user.Replace("\"", "\\\"") + "\", ";
            }
            if (pass != null)
            {
                connectMsg += "\"pass\":\"" + pass.Replace("\"", "\\\"") + "\", ";
            }
            connectMsg += "}";
        }

        private void Dispose(bool disposing)
        {
            ThrowIfDisposed();
            if (disposing)
            {
                cancellation.Cancel();
                SyncedIf(() => processor != null, () => {
                    var conn = connection;
                    connection = null;
                    if (conn != null)
                    {
                        conn.Dispose();
                    }
                });
                if (processor != null)
                {
                    processor.Wait();
                    processor.Dispose();
                    processor = null;
                }
                cancellation.Dispose();
                cancellation = null;
            }
            disposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException("NatsClient", "Already disposed");
            }
        }

        private void SyncedIf(Func<bool> condition, Action action)
        {
            if (condition())
            {
                lock (stateLock)
                {
                    if (condition())
                    {
                        action();
                    }
                }
            }
        }

        private Connection ConnectServer()
        {
            lock (stateLock)
            {
                if (connection != null)
                {
                    return connection;
                }
                if (processor == null)
                {
                    return null;
                }

                Connection newConn = null;
                try
                {
                    newConn = new Connection(serverAddr, serverPort);
                    newConn.Send(connectMsg);
                    foreach (var sub in subscriptions.Values)
                    {
                        SendSubscribe(newConn, sub);
                    }
                    connection = newConn;
                }
                catch (SocketException err)
                {
                    Trace.TraceError("NATS-CLIENT-EXCEPTION: {0}", err);
                }
                catch (IOException err)
                {
                    Trace.TraceError("NATS-CLIENT-EXCEPTION: {0}", err);
                }
                finally
                {
                    if (connection != newConn)
                    {
                        newConn.Dispose();
                    }
                }
                return connection;
            }
        }

        private bool WhenConnected(Action<Connection> action)
        {
            var conn = ConnectServer();
            if (conn != null)
            {
                try
                {
                    action(conn);
                    return true;
                }
                catch (IOException err)
                {
                    Trace.TraceError("NATS-CLIENT-EXCEPTION: {0}", err);
                    Disconnect(conn);
                }
                catch (ObjectDisposedException err)
                {
                    Trace.TraceError("NATS-CLIENT-EXCEPTION: {0}", err);
                }
            }
            return false;
        }

        private void Disconnect(Connection conn)
        {
            lock (stateLock)
            {
                if (connection != null && connection == conn)
                {
                    connection.Dispose();
                    connection = null;
                    return;
                }
            }
            conn.Dispose();
        }

        private void SendSubscribe(Connection conn, Subscription sub)
        {
            conn.Send("SUB " + sub.Topic + "   " + sub.Id);
        }

        private void SendUnsubscribe(Connection conn, long sid)
        {
            conn.Send("UNSUB " + sid + " 0");
        }

        private void SendPublish(Connection conn, string topic, string message)
        {
            conn.Send(new string[]{ "PUB " + topic + " " + message.Length, message });
        }

        private void Processing()
        {
            Debug.WriteLine("NATS-CLIENT-PROC: START");
            while (!cancellation.Token.IsCancellationRequested)
            {
                WhenConnected((conn) => ReceiveMessages(conn));
            }
            Debug.WriteLine("NATS-CLIENT-PROC: STOP");
        }

        private const int MAX_MSG_LEN = 0x80000;   // 1 MB

        private void ReceiveMessages(Connection conn)
        {
            Message msg = null;
            while (!cancellation.Token.IsCancellationRequested)
            {
                Debug.WriteLine("NATS-READ");
                if (msg != null)
                {
                    var buf = new char[msg.BodyLen];
                    conn.Reader.ReadBlock(buf, 0, buf.Length);
                    msg.Body = new string(buf);
                    msg = DispatchMessage(msg);
                }
                else
                {
                    var line = conn.Reader.ReadLine();
                    Debug.WriteLine(line, "NATS");

                    var tokens = line.Split(new char[] {' '});
                    switch (tokens[0])
                    {
                    case "MSG":
                        msg = ParseMsg(tokens);
                        if (msg != null && msg.BodyLen == 0)
                        {
                            msg.Body = string.Empty;
                            msg = DispatchMessage(msg);
                        }
                        else
                        {
                            Trace.TraceWarning("NATS-MSG: INVALID {0}", line);
                        }
                        break;
                    case "PING":
                        conn.Send("PONG");
                        break;
                    case "-ERR":
                        Trace.TraceError("NATS-ERR: {0}", line);
                        // Reconnect
                        Disconnect(conn);
                        return;
                    default:
                        // accepting "PONG", "INFO", "+OK"
                        break;
                    }
                }
            }
        }

        private Message ParseMsg(string[] tokens)
        {
            int len = 0, sid = 0;
            if (tokens.Length == 4)
            {
                if (int.TryParse(tokens[2], out sid) &&
                    int.TryParse(tokens[3], out len) &&
                    len >= 0 && len < MAX_MSG_LEN)
                {
                    return new Message(sid, tokens[1], null, len);
                }
            }
            else if (tokens.Length > 4)
            {
                if (int.TryParse(tokens[2], out sid) &&
                    int.TryParse(tokens[4], out len) &&
                    len >= 0 && len < MAX_MSG_LEN)
                {
                    return new Message(sid, tokens[1], tokens[3], len);
                }
            }
            return null;
        }

        private Message DispatchMessage(Message msg)
        {
            Debug.WriteLine("NATS-CLIENT-DISP: [{0}]({1}) {2}", msg.Topic, msg.Sid, msg.Body);

            Subscription sub = null;
            lock (stateLock)
            {
                subscriptions.TryGetValue(msg.Sid, out sub);
            }
            if (sub != null)
            {
                sub.Handler(msg.Body, msg.Reply);
            }
            return null;
        }
    }
}

