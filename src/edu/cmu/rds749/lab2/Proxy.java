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
import java.util.concurrent.atomic.AtomicInteger;

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
    private Object failureLock;
    private Object replyLock;


    /*Wont add this server to this list till the state is transferred*/
    private ArrayList<Server> serverList;
    private ConcurrentHashMap<Long,Server> serverMap;
    private ConcurrentHashMap<Integer,HashSet<Long>> replyMap;

    //This is used to keep track which server is involved with which request
    private ConcurrentHashMap<Integer,HashSet<Long>> requestServerMap;
    private AtomicInteger requestInProgress;
    private AtomicInteger numServers;

    public Proxy(Configuration config)
    {
        super(config);
        operationsLock = new Object();
        readReplyLock= new Object();
        changeReplyLock= new Object();
        failureLock = new Object();
        replyLock =  new Object();

        serverList = new ArrayList<Server>();
        serverMap = new ConcurrentHashMap<Long,Server>();

        replyMap = new ConcurrentHashMap<Integer,HashSet<Long>>();
        requestServerMap= new ConcurrentHashMap<Integer,HashSet<Long>>();

        requestInProgress = new AtomicInteger(0);
        numServers = new AtomicInteger(0);
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        ArrayList<Long> failedServers = new ArrayList<Long>();
        //Ensures Quiescence of System
        synchronized (operationsLock)
        {



            synchronized (failureLock)
            {

            /**
             * Ensuring system is not servicing any current
             * requests
             */
            if(serverList.size()!=0)
            {
                int requests = 1;
                do
                {
                    requests = requestInProgress.get();
                }
                while (requests > 0);
            }


                Server curr = new Server(stub);
                curr.setId(id);


                //get state
                if(serverList.size()!=0)
                {
                    for(Server active :serverList)
                    {

                        try
                        {
                            int state = active.getState();
                            curr.setState(state);
                            System.out.println("Registered id:" + id + " state:" + state);
                            break;
                        }
                        catch (Exception e)
                        {
                            System.out.println("Failed inside begin Quiesecne");
                            failedServers.add(active.getId());
                        }
                    }

                }

                serverMap.put(id, curr);
                serverList.add(curr);
            }
        }

        if(failedServers.size()!=0)
        {
            serversFailed(failedServers);
        }
    }

    @Override
    protected void beginReadBalance(int reqid)
    {
        synchronized (operationsLock)
        {
            List<Long> failedServerList =new ArrayList<Long>();

            synchronized (failureLock)
            {
                HashSet<Long> servicingServers = new HashSet<Long>();

                for (Server server : serverList) {

                    try {
                        server.beginReadBalance(reqid);
                        requestInProgress.incrementAndGet();
                        servicingServers.add(server.getId());
                    } catch (Exception e) {
                        failedServerList.add(server.getId());
                        e.printStackTrace();
                    }
                }

                if (failedServerList.size() == serverList.size()) {
                    System.out.println("No servers!");
                    System.out.println("Failed inside begin Read Balance");
                    clientProxy.RequestUnsuccessfulException(reqid);
                }


                if (servicingServers.size() != 0) {

                    requestServerMap.put(reqid, servicingServers);
                }

            }

            if (failedServerList.size() != 0)
            {
                serversFailed(failedServerList);

            }


        }
    }

    @Override
    protected void beginChangeBalance(int reqid, int update)
    {
        synchronized (operationsLock)
        {
            List<Long> failedServerList = new ArrayList<Long>();

            synchronized (failureLock)
            {

                HashSet<Long> servicingServers = new HashSet<Long>();

                for (Server server : serverList) {
                    try {
                        server.beginChangeBalance(reqid, update);
                        requestInProgress.incrementAndGet();
                        servicingServers.add(server.getId());
                    } catch (Exception e) {
                        failedServerList.add(server.getId());
                        System.out.println("Failed inside begin change Balance");
                        e.printStackTrace();
                    }
                }

                if (failedServerList.size() == serverList.size())
                {
                    System.out.println("No servers!");
                    clientProxy.RequestUnsuccessfulException(reqid);
                }

                if(servicingServers.size()!=0)
                {
                    requestServerMap.put(reqid, servicingServers);
                }
            }

            if (failedServerList.size() != 0)
            {
                serversFailed(failedServerList);

            }

        }
    }

    @Override
    protected void endReadBalance(long serverid, int reqid, int balance)
    {
        synchronized (replyLock)
        {
            requestInProgress.decrementAndGet();

            System.out.println("Reply from:" + serverid + " for:" + reqid +" balance=" + balance);

            if (replyMap.containsKey(reqid))
            {
                HashSet<Long> list = replyMap.get(reqid);
                list.add(serverid);

                /**
                 * All replies acquired. Can be removed from the map
                 */
                if(replyMap.get(reqid).size() == requestServerMap.get(reqid).size())
                {
                    replyMap.remove(reqid);
                }

                return;
            }

            HashSet<Long> newList = new HashSet<Long>();
            newList.add(serverid);
            replyMap.put(reqid,newList);
            clientProxy.endReadBalance(reqid, balance);

            /**
             * All replies acquired. Can be removed from the map
             */
            if(replyMap.get(reqid).size() == requestServerMap.get(reqid).size())
            {
                replyMap.remove(reqid);
            }

        }

    }

    @Override
    protected void endChangeBalance(long serverid, int reqid, int balance)
    {
        synchronized (replyLock)
        {
            System.out.println("Reply from:" + serverid + " for:" + reqid +" balance=" + balance);
            requestInProgress.decrementAndGet();

            if (replyMap.containsKey(reqid))
            {
                HashSet<Long> list = replyMap.get(reqid);
                list.add(serverid);

                /**
                 * All replies acquired. Can be removed from the map
                 */

                if(replyMap.get(reqid).size() == requestServerMap.get(reqid).size())
                {
                    replyMap.remove(reqid);
                }
                return;
            }

            HashSet<Long> newList = new HashSet<Long>();
            newList.add(serverid);
            replyMap.put(reqid,newList);
            clientProxy.endChangeBalance(reqid, balance);
            /**
             * All replies acquired. Can be removed from the map
             */
            if(replyMap.get(reqid).size() == requestServerMap.get(reqid).size())
            {
                replyMap.remove(reqid);
            }

        }
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        synchronized (failureLock)
        {

            super.serversFailed(failedServers);

            //keep Replies waiting.
            synchronized (replyLock)
            {
                for (Long serverId : failedServers)
                {
                    System.out.println("Servers Failed:" + serverId);

                    //Already removed
                    if(serverMap.containsKey(serverId)==false)
                    {
                        System.out.println("Server already Removed"+ serverId);
                        continue;
                    }

                    /**
                     *
                     * what to do with server Failures?
                     *
                     * Go through all the requests to see if that id is present in
                     * those requests, if yes, then see if that server has
                     * responded. If not, decrease the number of requests
                     * being serviced.(This will help in change of state
                     */

                    for(int requestId: replyMap.keySet())
                    {

                        HashSet<Long> replySet = requestServerMap.get(requestId);

                        /**
                         * Reply hasn't been received
                         */
                        if(replySet.contains(serverId)==false)
                        {
                            requestInProgress.decrementAndGet();
                            replySet.remove(serverId);
                        }

                    }

                    //Removing the failed server from the farm
                    Server current = serverMap.get(serverId);
                    serverList.remove(current);
                    serverMap.remove(serverId);
                    System.out.println("Server removed:" + serverId);

                }
            }


        }
    }




}
