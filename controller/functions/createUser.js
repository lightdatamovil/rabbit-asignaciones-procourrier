
import { getDbConfig, executeQuery } from "../../db.js";
import mysql2 from 'mysql2';

import { logRed } from "../../src/funciones/logsCustom.js";

export async function createUser(companyId) {
    const dbConfig = getDbConfig();
    const dbConnection = mysql2.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const username = `usuario_${companyId}`;
        const password = '78451296';

        const createUserSql = `CREATE USER IF NOT EXISTS ?@'%' IDENTIFIED BY ?`;
        const grantPrivilegesSql = `GRANT ALL PRIVILEGES ON \`asigna_data\`.* TO ?@'%'`;

        await executeQuery(dbConnection, createUserSql, [username, password]);
        await executeQuery(dbConnection, grantPrivilegesSql, [username]);

        return;
    } catch (error) {
        logRed(`Error al crear el usuario:  ${error.stack}`)

        throw error;
    }
}
