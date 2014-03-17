using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Nats
{
    public sealed class Options
    {
        public string Queue { get; private set; }

        public Options()
        {
            Queue = "";
        }

        public Options(string queue)
        {
            Queue = queue;
        }

    }


    sealed class Subscription
    {

        public int Id { get; private set; }

        public string Topic { get; private set; }

        public Options Options { get; private set; }

        public Action<int, string, string> Handler { get; private set; }

        public Subscription(int id, string topic, Options options, Action<int, string, string> handler)
        {
            Id = id;
            Topic = topic;
            Options = options;
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
        private TcpClient _client;

        public NetworkStream Stream { get; private set; }

        public StreamReader Reader { get; private set; }

        public Connection(string host, int port)
        {
            _client = new TcpClient(host, port);
            _client.NoDelay = true;
            Stream = _client.GetStream();
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
                if (_client != null)
                {
                    _client.Close();
                }
            }
            Reader = null;
            Stream = null;
            _client = null;
        }

        ~Connection()
        {
            Dispose(false);
        }
    }

    public sealed class NatsClient : IDisposable
    {
        private string _serverAddr = "localhost";
        private int    _serverPort = 4222;
        private string _connectMsg;

        private bool _disposed = false;
        private CancellationTokenSource _cancellation = new CancellationTokenSource();

        private int _sidBase = 0;
        private IDictionary<long, Subscription> _subscriptions = new Dictionary<long, Subscription>();
        private Connection _connection;
        private Thread _processor;
        private object _stateLock = new object();

        public NatsClient()
        {
        }

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
            SyncedIf(() => _processor == null, () => {
                _processor = new Thread(this.Processing);
                _processor.Start();
            });
        }

        public void Connect(Uri natsUrl)
        {
            ThrowIfDisposed();
            BuildConnectMsg(natsUrl);
            Connect();
        }

        /// <summary>
        /// Subscribe the specified topic and invokes messageHandler when message arrives.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name="options">
        /// Options.
        /// Current avialble options: queue
        /// </param>
        /// <param name='messageHandler'>
        /// Message handler.
        /// Three arguments are: subscriptionId, message, replyTopic
        /// replyTopic can be null if no reply is expected
        /// </param>
        /// <returns>
        /// Subscription Id
        /// </returns>
        /// <remarks>
        /// messageHandler may be invoked before this function returns as this is a multi-threading library
        /// </remarks>
        ///
        public int Subscribe(string topic, Options options, Action<int, string, string> messageHandler)
        {
            ThrowIfDisposed();
            var sub = new Subscription(Interlocked.Increment(ref _sidBase), topic, options, messageHandler);
            lock (_stateLock)
            {
                _subscriptions.Add(sub.Id, sub);
                if (_connection != null)
                {
                    SendSubscribe(_connection, sub);
                }
            }
            return sub.Id;
        }

        /// <summary>
        /// Subscribe the specified topic and invokes messageHandler when message arrives.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='messageHandler'>
        /// Message handler.
        /// Three arguments are: subscriptionId, message, replyTopic
        /// replyTopic can be null if no reply is expected
        /// </param>
        /// <returns>
        /// Subscription Id
        /// </returns>
        /// <remarks>
        /// messageHandler may be invoked before this function returns as this is a multi-threading library
        /// </remarks>
        ///
        public int Subscribe(string topic, Action<int, string, string> messagehandler)
        {
            return Subscribe(topic, new Options(), messagehandler);
        }

        /// <summary>
        /// Subscribe the specified topic and invokes messageHandler when message arrives.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name="options">
        /// Options.
        /// </param>
        /// <param name='messageHandler'>
        /// Message handler.
        /// Arguments are: message, replyTopic
        /// replyTopic can be null if no reply is expected
        /// </param>
        /// <returns>
        /// Subscription Id
        /// </returns>
        /// <remarks>
        /// messageHandler may be invoked before this function returns as this is a multi-threading library
        /// </remarks>
        public int Subscribe(string topic, Options options, Action<string, string> messageHandler)
        {
            return Subscribe(topic, options, (sid, msg, reply) => messageHandler(msg, reply));
        }

        /// <summary>
        /// Subscribe the specified topic and invokes messageHandler when message arrives.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='messageHandler'>
        /// Message handler.
        /// Arguments are: message, replyTopic
        /// replyTopic can be null if no reply is expected
        /// </param>
        /// <returns>
        /// Subscription Id
        /// </returns>
        /// <remarks>
        /// messageHandler may be invoked before this function returns as this is a multi-threading library
        /// </remarks>
        public int Subscribe(string topic, Action<string, string> messageHandler)
        {
            return Subscribe(topic, new Options(), messageHandler);
        }

        /// <summary>
        /// Subscribe the specified topic and invokes messageHandler when message arrives.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name="options">
        /// Options.
        /// </param>
        /// <param name='messageHandler'>
        /// Message handler.
        /// Argument is: message
        /// </param>
        /// <returns>
        /// Subscription Id
        /// </returns>
        /// <remarks>
        /// messageHandler may be invoked before this function returns as this is a multi-threading library
        /// </remarks>
        public int Subscribe(string topic, Options options, Action<string> messageHandler)
        {
            return Subscribe(topic, options, (sid, msg, reply) => messageHandler(msg));
        }

        /// <summary>
        /// Subscribe the specified topic and invokes messageHandler when message arrives.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='messageHandler'>
        /// Message handler.
        /// Argument is: message
        /// </param>
        /// <returns>
        /// Subscription Id
        /// </returns>
        /// <remarks>
        /// messageHandler may be invoked before this function returns as this is a multi-threading library
        /// </remarks>
        public int Subscribe(string topic, Action<string> messageHandler)
        {
            return Subscribe(topic, new Options(), messageHandler);
        }

        /// <summary>
        /// Unsubscribe subscription.
        /// </summary>
        /// <param name='sid'>
        /// subscription Id
        /// </param>
        public void Unsubscribe(int sid)
        {
            ThrowIfDisposed();
            lock (_stateLock)
            {
                if (_subscriptions.Remove(sid) && _connection != null)
                {
                    SendUnsubscribe(_connection, sid);
                }
            }
        }

        /// <summary>
        /// Publish message to the specified topic.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='message'>
        /// Message.
        /// </param>
        /// <param name='replyTopic'>
        /// If reply is expected, set this to the topic receiving reply.
        /// </param>
        /// <returns>
        /// true/false to indicate whether the message is published
        /// </returns>
        /// <remarks>
        /// It is possible the message is not published if connection to NATS is lost.
        /// It will NOT retry to publish the message before connection is established.
        /// </remarks>
        public bool Publish(string topic, string message, string replyTopic = null)
        {
            ThrowIfDisposed();
            lock (_stateLock)
            {
                return WhenConnected((conn) => SendPublish(conn, topic, message, replyTopic));
            }
        }

        /// <summary>
        /// Perform a request/response synchronously.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='message'>
        /// Message.
        /// </param>
        /// <param name='timeout'>
        /// Timeout.
        /// </param>
        /// <returns>
        /// Response received or null if publish fails or timeout.
        /// </returns>
        /// <remarks>
        /// A request is basically first subscribe to a unique inbox topic (using CreateInbox)
        /// and send a message to specified topic with replyTopic set to this unique inbox topic
        /// for receiving response.
        /// The inbox topic is automatically unsubscribed when the response is received.
        /// </remarks>
        public string Request(string topic, string message, int timeout = -1)
        {
            ThrowIfDisposed();
            string response = null;
            using (var waitHandle = new ManualResetEvent(false))
            {
                var inbox = CreateInbox();
                int subId = Subscribe(inbox, (sid, recvMsg, replyTopic) => {
                    Unsubscribe(sid);
                    response = recvMsg;
                    waitHandle.Set();
                });
                if (!Publish(topic, message, inbox) || !waitHandle.WaitOne(timeout))
                {
                    Unsubscribe(subId);
                }
            }
            return response;
        }

        /// <summary>
        /// Perform a request/response asynchronously.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='message'>
        /// Message.
        /// </param>
        /// <param name='responseHandler'>
        /// Response handler.
        /// </param>
        /// <returns>
        /// Subscription Id of inbox
        /// </returns>
        /// <remarks>
        /// As responseHandler expects subscription Id, when response arrives, it is directly
        /// forwarded to responseHandler, and the inbox is still subscribed.
        /// </remarks>
        public int Request(string topic, string message, Action<int, string, string> responseHandler)
        {
            ThrowIfDisposed();
            var inbox = CreateInbox();
            int subId = Subscribe(inbox, responseHandler);
            Publish(topic, message, inbox);
            return subId;
        }

        /// <summary>
        /// Perform a request/response asynchronously.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='message'>
        /// Message.
        /// </param>
        /// <param name='responseHandler'>
        /// Response handler.
        /// </param>
        /// <returns>
        /// Subscription Id of inbox
        /// </returns>
        /// <remarks>
        /// When response arrives, the inbox is unsubscribed before invoking responseHandler.
        /// </remarks>
        public int Request(string topic, string message, Action<string, string> responseHandler)
        {
            return Request(topic, message, (sid, msg, reply) => {
                Unsubscribe(sid);
                responseHandler(msg, reply);
            });
        }

        /// <summary>
        /// Perform a request/response asynchronously.
        /// </summary>
        /// <param name='topic'>
        /// Topic.
        /// </param>
        /// <param name='message'>
        /// Message.
        /// </param>
        /// <param name='responseHandler'>
        /// Response handler.
        /// </param>
        /// <returns>
        /// Subscription Id of inbox
        /// </returns>
        /// <remarks>
        /// When response arrives, the inbox is unsubscribed before invoking responseHandler.
        /// </remarks>
        public int Request(string topic, string message, Action<string> responseHandler)
        {
            return Request(topic, message, (sid, msg, reply) => {
                Unsubscribe(sid);
                responseHandler(msg);
            });
        }

        /// <summary>
        /// Generate a unique inbox topic
        /// </summary>
        public static string CreateInbox()
        {
            return "_INBOX." + Guid.NewGuid().ToString("N");
        }

        private void BuildConnectMsg(Uri natsUrl)
        {
            string user = null, pass = null;
            if (natsUrl != null)
            {
                _serverAddr = natsUrl.Host;
                if (natsUrl.Port > 0)
                {
                    _serverPort = natsUrl.Port;
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

            _connectMsg = "CONNECT { \"verbose\":false, \"pedantic\":false";
            if (user != null)
            {
                _connectMsg += ", \"user\":\"" + user.Replace("\"", "\\\"") + "\"";
            }
            if (pass != null)
            {
                _connectMsg += ", \"pass\":\"" + pass.Replace("\"", "\\\"") + "\"";
            }
            _connectMsg += " }";
        }

        private void Dispose(bool disposing)
        {
            ThrowIfDisposed();
            if (disposing)
            {
                _cancellation.Cancel();
                SyncedIf(() => _processor != null, () => {
                    var conn = _connection;
                    _connection = null;
                    if (conn != null)
                    {
                        conn.Dispose();
                    }
                });
                if (_processor != null)
                {
                    _processor.Join();
                    _processor = null;
                }
                _cancellation.Dispose();
                _cancellation = null;
            }
            _disposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("NatsClient", "Already disposed");
            }
        }

        private void SyncedIf(Func<bool> condition, Action action)
        {
            if (condition())
            {
                lock (_stateLock)
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
            lock (_stateLock)
            {
                if (_connection != null)
                {
                    return _connection;
                }
                if (_processor == null)
                {
                    return null;
                }

                Connection newConn = null;
                try
                {
                    newConn = new Connection(_serverAddr, _serverPort);
                    newConn.Send(_connectMsg);
                    foreach (var sub in _subscriptions.Values)
                    {
                        SendSubscribe(newConn, sub);
                    }
                    _connection = newConn;
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
                    if (_connection != newConn)
                    {
                        newConn.Dispose();
                    }
                }
                return _connection;
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
            lock (_stateLock)
            {
                if (_connection != null && _connection == conn)
                {
                    _connection.Dispose();
                    _connection = null;
                    return;
                }
            }
            conn.Dispose();
        }

        private void SendSubscribe(Connection conn, Subscription sub)
        {
            string queue = sub.Options.Queue;
            conn.Send("SUB " + sub.Topic + " " + queue + " " + sub.Id);
        }

        private void SendUnsubscribe(Connection conn, long sid)
        {
            conn.Send("UNSUB " + sid + " 0");
        }

        private void SendPublish(Connection conn, string topic, string message, string replyTopic)
        {
            string reply = string.IsNullOrEmpty(replyTopic) ? " " : (" " + replyTopic + " ");
            conn.Send(new string[]{ "PUB " + topic + reply + message.Length, message });
        }

        private void Processing()
        {
            Debug.WriteLine("NATS-CLIENT-PROC: START");
            while (!_cancellation.Token.IsCancellationRequested)
            {
                WhenConnected((conn) => ReceiveMessages(conn));
            }
            Debug.WriteLine("NATS-CLIENT-PROC: STOP");
        }

        private const int MAX_MSG_LEN = 0x80000;   // 1 MB

        private void ReceiveMessages(Connection conn)
        {
            Message msg = null;
            while (!_cancellation.Token.IsCancellationRequested)
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
            lock (_stateLock)
            {
                _subscriptions.TryGetValue(msg.Sid, out sub);
            }
            if (sub != null)
            {
                sub.Handler(sub.Id, msg.Body, msg.Reply);
            }
            return null;
        }
    }
}

