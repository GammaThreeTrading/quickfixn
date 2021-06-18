﻿using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace UnitTests
{
    [TestFixture]
    public class ODBCStoreTests
    {
        QuickFix.ODBCStore store;
        QuickFix.ODBCStoreFactory factory;

        QuickFix.SessionSettings settings;
        QuickFix.SessionID sessionID;



        [SetUp]
        public void setup()
        {
            sessionID = new QuickFix.SessionID("FIX.4.2", "SENDERCOMP", "TARGETCOMP");

            QuickFix.Dictionary config = new QuickFix.Dictionary();
            config.SetString(QuickFix.SessionSettings.CONNECTION_TYPE, "initiator");
            config.SetString(QuickFix.SessionSettings.ODBC_STORE_CONNECTION_STRING, "DSN=FIXSIM");

            settings = new QuickFix.SessionSettings();
            settings.Set(sessionID, config);
            factory = new QuickFix.ODBCStoreFactory(settings);

            store = (QuickFix.ODBCStore)factory.Create(sessionID);
        }

        void rebuildStore()
        {
            if (store != null)
            {
                store.Dispose();
            }

            store = (QuickFix.ODBCStore)factory.Create(sessionID);
        }

        [TearDown]
        public void teardown()
        {
            store.Dispose();
        }

        [Test]
        public void testPrefixForSessionWithSubsAndLoc()
        {
            QuickFix.SessionID sessionIDWithSubsAndLocation = new QuickFix.SessionID("FIX.4.2", "SENDERCOMP", "SENDERSUB", "SENDERLOC", "TARGETCOMP", "TARGETSUB", "TARGETLOC");
            Assert.That(QuickFix.FileStore.Prefix(sessionIDWithSubsAndLocation), Is.EqualTo("FIX.4.2-SENDERCOMP_SENDERSUB_SENDERLOC-TARGETCOMP_TARGETSUB_TARGETLOC"));

            QuickFix.SessionID sessionIDWithSubsNoLocation = new QuickFix.SessionID("FIX.4.2", "SENDERCOMP", "SENDERSUB", "TARGETCOMP", "TARGETSUB");
            Assert.That(QuickFix.FileStore.Prefix(sessionIDWithSubsNoLocation), Is.EqualTo("FIX.4.2-SENDERCOMP_SENDERSUB-TARGETCOMP_TARGETSUB"));
        }

        [Test]
        public void getNextSenderMsgSeqNumTest()
        {
            Assert.AreEqual(1, store.GetNextSenderMsgSeqNum());
            store.SetNextSenderMsgSeqNum(5);
            Assert.AreEqual(5, store.GetNextSenderMsgSeqNum());
            rebuildStore();
            Assert.AreEqual(5, store.GetNextSenderMsgSeqNum());
        }

        [Test]
        public void incNextSenderMsgSeqNumTest()
        {
            int nNextSender = store.GetNextSenderMsgSeqNum();
            store.IncrNextSenderMsgSeqNum();
            Assert.AreEqual(nNextSender+1, store.GetNextSenderMsgSeqNum());
            rebuildStore();
            Assert.AreEqual(nNextSender + 1, store.GetNextSenderMsgSeqNum());
        }

        [Test]
        public void getNextTargetMsgSeqNumTest()
        {
            Assert.AreEqual(1, store.GetNextTargetMsgSeqNum());
            store.SetNextTargetMsgSeqNum(6);
            Assert.AreEqual(6, store.GetNextTargetMsgSeqNum());
            rebuildStore();
            Assert.AreEqual(6, store.GetNextTargetMsgSeqNum());
        }

        [Test]
        public void incNextTargetMsgSeqNumTest()
        {
            int nNextTarget = store.GetNextTargetMsgSeqNum();

            store.IncrNextTargetMsgSeqNum();
            Assert.AreEqual(nNextTarget+1, store.GetNextTargetMsgSeqNum());
            rebuildStore();
            Assert.AreEqual(nNextTarget+1, store.GetNextTargetMsgSeqNum());
        }

        [Test]
        public void resetTest()
        {
            // seq nums reset
            store.SetNextTargetMsgSeqNum(5);
            store.SetNextSenderMsgSeqNum(4);
            store.Reset();
            Assert.AreEqual(1, store.GetNextTargetMsgSeqNum());
            Assert.AreEqual(1, store.GetNextSenderMsgSeqNum());

            // Check that messages do not persist after reset
            store.Set(1, "dude");
            store.Set(2, "pude");
            store.Set(3, "ok");
            store.Set(4, "ohai");

            store.Reset();

            var msgs = new List<string>();
            store.Get(2, 3, msgs);
            Assert.That(msgs, Is.Empty);
        }

        [Test]
        public void CreationTimeTest()
        {
            DateTime d1 = store.CreationTime.Value;
            rebuildStore();
            DateTime d2 = store.CreationTime.Value;
            Util.UtcDateTimeSerializerTests.AssertHackyDateTimeEquality(d1, d2);

            Thread.Sleep(1000);
            store.Reset();
            DateTime d3 = store.CreationTime.Value;
            Assert.AreEqual(-1, DateTimeOffset.Compare(d1, d3)); // e.g. d1 is earlier than d3
        }


        [Test]
        public void getTest()
        {
            store.Set(1, "dude");
            store.Set(2, "pude");
            store.Set(3, "ok");
            store.Set(4, "ohai");

            var msgs = new List<string>();
            store.Get(2, 3, msgs);
            var expected = new List<string>() { "pude", "ok" };

            Assert.AreEqual(expected, msgs);

            rebuildStore();

            msgs = new List<string>();
            store.Get(2, 3, msgs);

            Assert.AreEqual(expected, msgs);
        }

    }


}