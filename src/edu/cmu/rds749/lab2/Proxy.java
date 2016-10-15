package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;

import java.util.List;


/**
 * Created by jiaqi on 8/28/16.
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    public Proxy(Configuration config)
    {
        super(config);
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {

    }

    @Override
    protected void beginReadBalance(int reqid)
    {
        System.out.println("(In Proxy)");
    }

    @Override
    protected void beginChangeBalance(int reqid, int update)
    {
        System.out.println("(In Proxy)");
    }

    @Override
    protected void endReadBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy)");
    }

    @Override
    protected void endChangeBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy)");
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }
}
