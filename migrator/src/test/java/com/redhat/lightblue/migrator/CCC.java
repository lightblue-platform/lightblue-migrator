package com.redhat.lightblue.migrator;

import java.util.Date;

public class CCC extends ConsistencyCheckerController {

    private static Date now = new Date();

    public CCC(Controller controller, MigrationConfiguration migrationConfiguration) {
        super(controller, migrationConfiguration);
    }

    public static void setNow(Date d) {
        now = d;
    }

    protected Date getNow() {
        return now;
    }
}
