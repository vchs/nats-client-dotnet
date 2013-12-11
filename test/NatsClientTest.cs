using System;
using System.Threading;
using NUnit.Framework;
using Nats;

namespace NatsClientTest
{
    [TestFixture]
    public class NatsClientTest
    {
        private Uri NatsUrl = new Uri("nats://localhost");

        [Test]
        public void NatsClient_PubSub()
        {
            string recvMsg = null;
            using (var nats = new NatsClient(NatsUrl))
            using (var waitHandle = new ManualResetEvent(false))
            {
                nats.Connect();
                nats.Subscribe("test", (msg, source) => {
                    Console.WriteLine("Received: {0}", msg);
                    recvMsg = msg;
                    waitHandle.Set();
                });
                nats.Publish("test", "Hello");
                waitHandle.WaitOne(1000);
            }
            Assert.AreEqual("Hello", recvMsg);
        }

        [Test]
        public void NatsClient_Request()
        {
            string response = null;
            using (var natsSvc = new NatsClient(NatsUrl))
            using (var natsClt = new NatsClient(NatsUrl))
            using (var waitHandle = new ManualResetEvent(false))
            {
                natsSvc.Connect();
                natsSvc.Subscribe("test-request", (msg, source) => {
                    Console.WriteLine("Request: {0}", msg);
                    if (string.IsNullOrEmpty(source))
                    {
                        waitHandle.Set();
                    }
                    else
                    {
                        natsSvc.Publish(source, msg + "World");
                    }
                });
                natsClt.Connect();
                // Ensure server is ready
                natsClt.Publish("test-request", "Ping");
                waitHandle.WaitOne(1000);
                // Now, send request
                response = natsClt.Request("test-request", "Hello", 1000);
            }
            Assert.AreEqual("HelloWorld", response);
        }

        [Test]
        public void NatsClient_RequestAsync()
        {
            string response = null;
            using (var natsSvc = new NatsClient(NatsUrl))
            using (var natsClt = new NatsClient(NatsUrl))
            using (var waitHandle = new AutoResetEvent(false))
            {
                natsSvc.Connect();
                natsSvc.Subscribe("test-request", (msg, source) => {
                    Console.WriteLine("Request: {0}", msg);
                    if (string.IsNullOrEmpty(source))
                    {
                        waitHandle.Set();
                    }
                    else
                    {
                        natsSvc.Publish(source, msg + "World");
                    }
                });
                natsClt.Connect();
                // Ensure server is ready
                natsClt.Publish("test-request", "Ping");
                waitHandle.WaitOne(1000);
                // Now, send request
                natsClt.Request("test-request", "Hello", (msg) => {
                    response = msg;
                    waitHandle.Set();
                });
                waitHandle.WaitOne(1000);
            }
            Assert.AreEqual("HelloWorld", response);
        }
    }
}
