package org.mule.modules.couchbase;

import net.spy.memcached.internal.OperationFuture;

import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.modules.couchbase.config.ConnectorConfig;

@Connector(name="couchbase", friendlyName="Couchbase")
public class CouchbaseConnector {

	@Config
    ConnectorConfig config;

    /**
     * Custom processor
     *
     * @param friend Name to be used to generate a greeting message.
     * @return A greeting message
     */
    /**
     * Get with a single key and decode using the default transcoder.
     * <p/>
     * {@sample.xml ../../../doc/Couchbase-connector.xml.sample couchbase:get}
     *
     * @param key The key of the JSON object you want to retrieve.
     * @return String The JSON object.
     */
    @Processor
    public String get(String key) {
        return getConfig().getClient().get(key).toString();
    }
    
    /**
     * Store an object given a specific key.
     * <p/>
     * {@sample.xml ../../../doc/Couchbase-connector.xml.sample couchbase:store}
     *
     * @param key The key of the JSON object you want to store.
     * @param value The value in JSON (as a string).
     */    
    @Processor
    public void store(String key, String value)
    {
        OperationFuture<Boolean> setOp = getConfig().getClient().set(key, Integer.MAX_VALUE, value);

        try 
        {
            if (setOp.get().booleanValue()) 
            {
                System.out.println("Set Succeeded");
            } 
            else 
            {
                System.err.println("Set failed: " + setOp.getStatus().getMessage());
            }
        } 
        catch (Exception e) 
        {
            System.err.println("Exception while doing set: " + e.getMessage());
        }           
    }
    
    /**
     * Remove an object given a specific key.
     * <p/>
     * {@sample.xml ../../../doc/Couchbase-connector.xml.sample couchbase:remove}
     *
     * @param key The key of the JSON object you want to remove.
     */    
    @Processor
    public void remove(String key)
    {
        getConfig().getClient().delete(key);
    }    

    public ConnectorConfig getConfig() {
        return config;
    }

    public void setConfig(ConnectorConfig config) {
        this.config = config;
    }

}