package org.mule.modules.couchbase.config;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.mule.api.ConnectionException;
import org.mule.api.annotations.components.ConnectionManagement;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.TestConnectivity;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;

import com.couchbase.client.CouchbaseClient;

@ConnectionManagement(friendlyName = "Configuration", configElementName = "config")
public class ConnectorConfig {

    private CouchbaseClient client; 
    
    @Connect
    @TestConnectivity(active=false)
    public void connect(
            @Default("") @Optional final String user,
            @Default("") @Optional @Password String password,
            @Default("8093") @Optional final int port,
            @ConnectionKey final String host,            
            @ConnectionKey String bucketName) throws ConnectionException {

        String uriString = "http://" + host + ":" + port + "/" + bucketName;
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(uriString));

        password = (password == null) ? "" : password;
        try {
            if (user == null || user.equals("")) {
                setClient(new CouchbaseClient(uris, bucketName, password));
            } else {
                setClient(new CouchbaseClient(uris, bucketName, user, password));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Disconnect
     */
    @Disconnect
    public void disconnect() {
        getClient().shutdown();
        setClient(null);      
    }

    /**
     * Are we connected
     */
    @ValidateConnection
    public boolean isConnected() {
        return getClient() != null;
    }

    /**
     * Are we connected
     */
    @ConnectionIdentifier
    public String connectionId() {
        return getClient().toString();
    }
    /**
     * Set Couchbase client
     *
     * @param client the Couchbase client
     */
    public void setClient(CouchbaseClient client) {
        this.client = client;
    }

    /**
     * Get Couchbase client
     */
    public CouchbaseClient getClient() {
        return this.client;
    }
}