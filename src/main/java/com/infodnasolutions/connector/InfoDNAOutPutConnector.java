package com.infodnasolutions.connector;


import com.infodnasolutions.connector.constants.*;
import com.infodnasolutions.connector.dao.*;
import com.infodnasolutions.connector.vo.DocumentMetaInfo;
import org.apache.manifoldcf.agents.interfaces.IOutputAddActivity;
import org.apache.manifoldcf.agents.interfaces.IOutputNotifyActivity;
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.agents.output.BaseOutputConnector;
import org.apache.manifoldcf.agents.system.Logging;
import org.apache.manifoldcf.agents.system.ManifoldCF;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.examples.docs4u.D4UException;
import org.apache.manifoldcf.examples.docs4u.D4UFactory;
import org.apache.manifoldcf.examples.docs4u.Docs4UAPI;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class InfoDNAOutPutConnector extends BaseOutputConnector {

    protected final static String PARAMETER_REPOSITORY_ROOT = "rootdirectory";

    //	Database properties
    protected final static String PARAMETER_USERNAME = "userName";
    protected final static String PARAMETER_PASSWORD = "password";

    protected final static String PARAMETER_HOST_NAME = "hostName";
    protected final static String PARAMETER_PORT = "port";
    protected final static String PARAMETER_SERVICE_NAME = "serviceName";
    protected final static String PARAMETER_SID = "sid";

    protected final static String PARAMETER_TABLE_NAME = "tableName";

    protected final static long SESSION_EXPIRATION_MILLISECONDS = 300000L;
    protected String rootDirectory = null;
    protected Docs4UAPI session = null;
    protected long sessionExpiration = -1L;

    /**
     * Save activity
     */
    protected final static String ACTIVITY_SAVE = "save";
    /**
     * Delete activity
     */
    protected final static String ACTIVITY_DELETE = "delete";

    /**
     * Ingestion activity
     */
    public final static String INGEST_ACTIVITY = "document ingest";

    public InfoDNAOutPutConnector() {
        super();
    }

    @Override
    public String[] getActivitiesList() {
        return new String[]{ACTIVITY_SAVE, ACTIVITY_DELETE};
    }

    @Override
    public String check() throws ManifoldCFException {
        try {
            Docs4UAPI currentSession = getSession();
            try {
                currentSession.sanityCheck();
            } catch (D4UException e) {
                Logging.ingest.warn("Docs4U: Error checking repository: " + e.getMessage(), e);
                return "Error: " + e.getMessage();
            }
            return super.check();
        } catch (ServiceInterruption e) {
            return "Transient error: " + e.getMessage();
        }
    }


    @Override
    public void connect(ConfigParams configParams) {
        super.connect(configParams);
        rootDirectory = configParams.getParameter(PARAMETER_REPOSITORY_ROOT);
    }

    @Override
    public void disconnect() throws ManifoldCFException {
        expireSession();
        rootDirectory = null;
        super.disconnect();
    }

    @Override
    public void outputConfigurationHeader(IThreadContext threadContext, IHTTPOutput out, Locale locale,
                                          ConfigParams parameters, List<String> tabsArray) throws ManifoldCFException, IOException {
        tabsArray.add("Repository");
        super.outputConfigurationHeader(threadContext, out, locale, parameters, tabsArray);
    }

    @Override
    public void outputConfigurationBody(IThreadContext threadContext, IHTTPOutput out, Locale locale,
                                        ConfigParams parameters, String tabName) throws ManifoldCFException, IOException {

        if (tabName.equals("Repository")) {

            out.println("<div> RootDirectory: <input type=\"text\" name=\"rootdirectory\"/> </div>");

            //			Database  properties
            out.println("<div> HostName:    <input type=\"text\" name=\"hostName\"/> </div>");
            out.println("<div> Port:        <input type=\"text\" name=\"port\"/>  </div>");
            out.println("<div> UserName:    <input type=\"text\" name=\"userName\"/>  </div>");
            out.println("<div> Password:    <input type=\"text\" name=\"password\"/> </div>");
            out.println("<div> ServiceName: <input type=\"text\" name=\"serviceName\"/>  </div>");
            out.println("<div> SID: <input type=\"text\" name=\"sid\"/>  </div>");
            out.println("<div> TableName:   <input type=\"text\" name=\"tableName\"/>  </div>");
        }
        super.outputConfigurationBody(threadContext, out, locale, parameters, tabName);
    }

    @Override
    public void poll() throws ManifoldCFException {
        if (session != null) {
            if (System.currentTimeMillis() >= sessionExpiration)
                expireSession();
        }
    }

    @Override
    public String processConfigurationPost(IThreadContext threadContext, IPostParameters variableContext,
                                           ConfigParams parameters) throws ManifoldCFException {
        String repositoryRoot = variableContext.getParameter(PARAMETER_REPOSITORY_ROOT);

        //		Database properties
        String userName = variableContext.getParameter(PARAMETER_USERNAME);
        String password = variableContext.getParameter(PARAMETER_PASSWORD);

        String hostName = variableContext.getParameter(PARAMETER_HOST_NAME);
        String port = variableContext.getParameter(PARAMETER_PORT);
        String serviceName = variableContext.getParameter(PARAMETER_SERVICE_NAME);
        String sid = variableContext.getParameter(PARAMETER_SID);

        String tableName = variableContext.getParameter(PARAMETER_TABLE_NAME);

        if (repositoryRoot != null) {
            parameters.setParameter(PARAMETER_REPOSITORY_ROOT, repositoryRoot);
            parameters.setParameter(PARAMETER_USERNAME, userName);
            parameters.setParameter(PARAMETER_PASSWORD, password);

            parameters.setParameter(PARAMETER_HOST_NAME, hostName);
            parameters.setParameter(PARAMETER_PORT, port);
            parameters.setParameter(PARAMETER_TABLE_NAME, tableName);

            parameters.setParameter(PARAMETER_SID, sid);
            parameters.setParameter(PARAMETER_SERVICE_NAME, serviceName);
        }
        return null;
    }

    @Override
    public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out, Locale locale, ConfigParams parameters)
            throws ManifoldCFException, IOException {
        out.print("<div> RootDirectory:" + "\t" + parameters.getParameter(PARAMETER_REPOSITORY_ROOT) + "</div>");

        out.print("<div> Username:" + "\t" + parameters.getParameter(PARAMETER_USERNAME) + "</div>");
        out.print("<div> password:" + "\t" + parameters.getParameter(PARAMETER_PASSWORD) + "</div>");

        out.print("<div> HostName:" + "\t" + parameters.getParameter(PARAMETER_HOST_NAME) + "</div>");
        out.print("<div> Port:" + "\t" + parameters.getParameter(PARAMETER_PORT) + "</div>");

        out.print("<div> ServiceName:" + "\t" + parameters.getParameter(PARAMETER_SERVICE_NAME) + "</div>");
        out.print("<div> SID:" + "\t" + parameters.getParameter(PARAMETER_SID) + "</div>");

        out.print("<div> TableName:" + "\t" + parameters.getParameter(PARAMETER_TABLE_NAME) + "</div>");

        out.print("<div> DataBasename:" + "\t" + ManifoldCF.getMasterDatabaseName() + "</div>");
        out.print("<div> Username:" + "\t" + ManifoldCF.getMasterDatabaseUsername() + "</div>");
        out.print("<div> Password:" + "\t" + ManifoldCF.getMasterDatabasePassword() + "</div>");


        super.viewConfiguration(threadContext, out, locale, parameters);
    }


    /**
     * Expire any current session.
     */
    protected void expireSession() {
        session = null;
        sessionExpiration = -1L;
    }


    /**
     * Get the current session, or create one if not valid.
     */
    protected Docs4UAPI getSession() throws ManifoldCFException, ServiceInterruption {
        if (session == null) {
            try {
                session = D4UFactory.makeAPI(rootDirectory);

            } catch (D4UException e) {
                Logging.ingest.warn("Docs4U: Session setup error: " + e.getMessage(), e);
                throw new ManifoldCFException("Session setup error: " + e.getMessage(), e);
            }
        }
        sessionExpiration = System.currentTimeMillis() + SESSION_EXPIRATION_MILLISECONDS;
        return session;
    }


    @Override
    public void noteJobComplete(IOutputNotifyActivity activities) throws ManifoldCFException, ServiceInterruption {
        //System.out.println("==in noteJobComplete==");
        super.noteJobComplete(activities);
    }


    /**
     * Request arbitrary connector information.
     * This method is called directly from the API in order to allow API users to perform any one of several
     * connector-specific queries.  These are usually used to create external UI's.  The connector will be
     * connected before this method is called.
     *
     * @param output  is the response object, to be filled in by this method.
     * @param command is the command, which is taken directly from the API request.
     * @return true if the resource is found, false if not.  In either case, output may be filled in.
     */
    @Override
    public boolean requestInfo(Configuration output, String command)
            throws ManifoldCFException {
        // Look for the commands we know about
       // System.out.println("==in requestInfo==");
        if (command.equals("metadata")) {
            // Use a try/catch to capture errors from repository communication
            try {
                // Get the metadata names
                String[] metadataNames = getMetadataNames();
                // Code these up in the output, in a form that yields decent JSON
                int i = 0;
                while (i < metadataNames.length) {
                    String metadataName = metadataNames[i++];
                    // Construct an appropriate node
                    ConfigurationNode node = new ConfigurationNode("metadata");
                    ConfigurationNode child = new ConfigurationNode("name");
                    child.setValue(metadataName);
                    node.addChild(node.getChildCount(), child);
                    output.addChild(output.getChildCount(), node);
                }
            } catch (ServiceInterruption e) {
                ManifoldCF.createServiceInterruptionNode(output, e);
            } catch (ManifoldCFException e) {
                ManifoldCF.createErrorNode(output, e);
            }
        } else
            return super.requestInfo(output, command);
        return true;
    }

    /**
     * Get an ordered list of metadata names.
     */
    protected String[] getMetadataNames() throws ManifoldCFException, ServiceInterruption {
        //System.out.println("in getMetadataNames");
        Docs4UAPI currentSession = getSession();
        try {
            String[] rval = currentSession.getMetadataNames();
            java.util.Arrays.sort(rval);
            return rval;
        } catch (InterruptedException e) {
            throw new ManifoldCFException(e.getMessage(), e, ManifoldCFException.INTERRUPTED);
        } catch (D4UException e) {
            throw new ManifoldCFException(e.getMessage(), e);
        }
    }


    @Override
    public void install(IThreadContext threadContext) throws ManifoldCFException {

        ConnectorDao postgresDatabaseManager = new ConnectorDao(threadContext, DatabaseConstants.DATABASE_NAME, DatabaseConstants.DATABASE_USER_NAME, DatabaseConstants.DATABASE_USER_PASSWORD);
        postgresDatabaseManager.openDatabase();
        postgresDatabaseManager.beginTransaction();
        StringSet allTables = postgresDatabaseManager.getAllTables(null, null);
        if (!allTables.contains(DatabaseConstants.DATABASE_TABLE_NAME)) {
            postgresDatabaseManager.createDatabaseTable();
        } else {
            System.out.println("Table already exist with name" + DatabaseConstants.DATABASE_TABLE_NAME);
        }
        postgresDatabaseManager.endTransaction();
        postgresDatabaseManager.closeDatabase();
    }

    public int addOrReplaceDocumentWithException(String documentURI, VersionContext outputDescription, RepositoryDocument document, String authorityNameString, IOutputAddActivity activities) throws ManifoldCFException, ServiceInterruption, IOException {

        DocumentMetaInfo documentMetaInfo = new DocumentMetaInfo();
        documentMetaInfo.setFileName(document.getFileName());
        documentMetaInfo.setFileCreatedDate(document.getCreatedDate());
        documentMetaInfo.setFileModifiedDate(document.getModifiedDate());
        documentMetaInfo.setFileSize(document.getOriginalSize());

        IThreadContext threadContext = ThreadContextFactory.make();
        ConnectorDao postgresDatabaseManager = new ConnectorDao(threadContext, DatabaseConstants.DATABASE_NAME, DatabaseConstants.DATABASE_USER_NAME, DatabaseConstants.DATABASE_USER_PASSWORD);

        postgresDatabaseManager.beginTransaction();
        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put(DatabaseConstants.FILE_NAME, documentMetaInfo.getFileName());
        paramMap.put(DatabaseConstants.FILE_CREATED_DATE, documentMetaInfo.getFileCreatedDate());
        paramMap.put(DatabaseConstants.FILE_SIZE, documentMetaInfo.getFileSize());
        paramMap.put(DatabaseConstants.FILE_MODIFIED_DATE, documentMetaInfo.getFileModifiedDate());
        try {
            postgresDatabaseManager.insertFileInfo(paramMap);
            postgresDatabaseManager.endTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public void deinstall(IThreadContext threadContext) throws ManifoldCFException {
        super.deinstall(threadContext);
    }
}