package com.redhat.lightblue.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Date;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.ExpressionOperation;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.expression.update.SetUpdate;
import com.redhat.lightblue.client.expression.update.PathValuePair;
import com.redhat.lightblue.client.expression.update.AppendUpdate;
import com.redhat.lightblue.client.expression.update.LiteralRValue;
import com.redhat.lightblue.client.expression.update.ObjectRValue;
import com.redhat.lightblue.client.expression.update.ForeachUpdate;
import com.redhat.lightblue.client.util.ClientConstants;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;
import static com.redhat.lightblue.client.projection.FieldProjection.includeField;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;

public abstract class Migrator extends Thread {

    private static final Logger LOGGER=LoggerFactory.getLogger(Migrator.class);

    private MigratorController controller;
    private MigrationJob migrationJob;
    private ActiveExecution activeExecution;

    public void setController(MigratorController c) {
        this.controller=c;
    }

    public void setMigrationJob(MigrationJob m) {
        migrationJob=m;
    }

    public void setActiveExecution(ActiveExecution e) {
        activeExecution=e;
    }

    public MigrationJob getMigrationJob() {
        return migrationJob;
    }

    public ActiveExecution getActiveExecution() {
        return activeExecution;
    }

    public abstract String migrate();
    
    @Override
    public final void run() {
        // First update the migration job, mark its status as being
        // processed, so it doesn't show up in other controllers'
        // tasks lists
        LightblueClient lbClient=controller.getController().getLightblueClient();
        DataUpdateRequest updateRequest = new DataUpdateRequest("migrationJob", null);
        updateRequest.where(withValue("_id",ExpressionOperation.EQ, migrationJob.get_id()));
        updateRequest.returns(includeField("_id"));

        // State is active
        updateRequest.updates(new SetUpdate(new PathValuePair("state",new LiteralRValue(MigrationJob.STATE_ACTIVE))),
                              // Add a new execution element
                              new AppendUpdate("executions",new ObjectRValue(new HashMap())),
                              // Execution id
                              new SetUpdate(new PathValuePair("executions.-1.activeExecutionId",new LiteralRValue(activeExecution.get_id()))),
                              // Start date
                              new SetUpdate(new PathValuePair("executions.-1.actualStartDate",new LiteralRValue(ClientConstants.getDateFormat().format(activeExecution.getStartTime())))),
                              // Status
                              new SetUpdate(new PathValuePair("executions.-1.status",new LiteralRValue(MigrationJob.STATE_ACTIVE))));       
        LOGGER.debug("Marking job {} as active",migrationJob.get_id());
        LightblueResponse response;
        try {
            response = lbClient.data(updateRequest);
            if(!response.hasError()) {
                // Do the migration
                String error=migrate();

                // If there is error, 'error' will contain a messages, otherwise it'll be null
                // Update the state
                updateRequest=new DataUpdateRequest("migrationJob",null);
                updateRequest.where(withValue("_id = "+migrationJob.get_id()));
                updateRequest.returns(includeField("_id"));
                updateRequest.updates(new SetUpdate(new PathValuePair("state",new LiteralRValue(error==null? MigrationJob.STATE_COMPLETED:
                                                                                                MigrationJob.STATE_FAILED))) ,
                                      new ForeachUpdate("executions",
                                                        withValue("activeExecutionId",ExpressionOperation.EQ,activeExecution.get_id()),
                                                        new SetUpdate(new PathValuePair("status",new LiteralRValue(error==null?MigrationJob.STATE_COMPLETED:
                                                                                                                   MigrationJob.STATE_FAILED)),
                                                                      new PathValuePair("errorMsg",new LiteralRValue(error==null?"":error)),
                                                                      new PathValuePair("actualEndDate", new LiteralRValue(ClientConstants.getDateFormat().format(new Date()))))));

                response=lbClient.data(updateRequest);
                if(response.hasError())
                    throw new RuntimeException("Failed to update:"+response);

            } else
                throw new RuntimeException("Failed to update:"+response);
        } catch (Exception e) {
            LOGGER.error("Cannot update job {}, {}",migrationJob.get_id(),e);
        }
    }
}

