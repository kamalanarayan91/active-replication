package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;
import rds749.ClientCallbacksPrx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiaqi on 8/28/16.
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    public static final long ERRORID = -1;
    private Object operationsLock;
    private Object readReplyLock;
    private Object changeReplyLock;

    private HashSet<Integer> servicedRequests;

    /*Wont add this server to this list till the state is transferred*/
    private ArrayList<Server> serverList;
    private ConcurrentHashMap<Long,Server> serverMap;
    private ConcurrentHashMap<Integer,Boolean> replyMap;

    public Proxy(Configuration config)
    {
        super(config);
        operationsLock = new Object();
        servicedRequests = new HashSet<Integer>();
        serverList = new ArrayList<Server>();
        serverMap = new ConcurrentHashMap<Long,Server>();
        replyMap = new ConcurrentHashMap<Integer,Boolean>();
        readReplyLock= new Object();
        changeReplyLock= new Object();
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        synchronized (operationsLock)
        {
            //Change State not implemented as of now.
            Server curr = new Server(stub);
            curr.setId(id);
            serverMap.put(id,curr);
            serverList.add(curr);

        }
    }

    @Override
    protected void beginReadBalance(int reqid)
    {
        synchronized (operationsLock)
        {
            for(Server server : serverList)
            {
                try
                {
                    server.beginReadBalance(reqid);
                }
                catch (BankAccountStub.NoConnectionException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected void beginChangeBalance(int reqid, int update)
    {
        synchronized (operationsLock)
        {
            for(Server server : serverList)
            {
                try
                {
                    server.beginChangeBalance(reqid,update);
                }
                catch (BankAccountStub.NoConnectionException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected void endReadBalance(long serverid, int reqid, int balance)
    {
        synchronized (readReplyLock)
        {
            if (replyMap.containsKey(reqid))
            {
                return;
            }

            replyMap.put(reqid,true);
            clientProxy.endReadBalance(reqid, balance);

        }

    }

    @Override
    protected void endChangeBalance(long serverid, int reqid, int balance)
    {
        synchronized (changeReplyLock)
        {
            if (replyMap.containsKey(reqid))
            {
                return;
            }

            replyMap.put(reqid,true);
            clientProxy.endChangeBalance(reqid, balance);

        }
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }


}
