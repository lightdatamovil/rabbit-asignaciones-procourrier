import { executeQuery, getDbConfig, updateRedis } from '../../../db.js';

import { logCyan, logRed, logYellow } from '../../../src/funciones/logsCustom.js';
import { insertAsignacionesDB } from '../../functions/insertAsignacionesDB.js';
import { createAssignmentsTable } from '../../functions/createAssignmentsTable.js';
import { createUser } from '../../functions/createUser.js';
import { crearLog } from '../../../src/funciones/crear_log.js';
import mysql2 from 'mysql2/promise';

export async function asignar(startTime, dbConnection, company, userId, driverId, deviceFrom, shipmentId, body) {
    const dbConfigLocal = getDbConfig();
    const dbConnectionLocal = await mysql2.createConnection(dbConfigLocal);

    try {
        const estadoQuery = `SELECT estado FROM envios_historial WHERE superado=0 AND elim=0 AND didEnvio = ?`;
        const estadoRows = await executeQuery(dbConnection, estadoQuery, [shipmentId]);

        if (estadoRows.length === 0) {
            throw new Error("No se pudo obtener el estado del paquete.");
        }
        logCyan("Obtengo el estado del paquete");

        const estado = estadoRows[0].estado;

        await createAssignmentsTable(dbConnectionLocal, company.did, dbConnection);
        logCyan("Creo la tabla de asignaciones");

        await createUser(dbConnectionLocal, company.did, dbConnection);
        logCyan("Creo la tabla de usuarios");

        const insertSql = `INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES (?, ?, ?, ?, ?, ?)`;
        const result = await executeQuery(dbConnection, insertSql, ["", driverId, shipmentId, estado, userId, deviceFrom]);
        logCyan("Inserto en la tabla de asignaciones");

        const did = result.insertId;

        const queries = [
            { sql: `UPDATE envios_asignaciones SET did = ? WHERE superado=0 AND elim=0 AND id = ?`, values: [did, did] },
            { sql: `UPDATE envios_asignaciones SET superado = 1 WHERE superado=0 AND elim=0 AND didEnvio = ? AND did != ?`, values: [shipmentId, did] },
            { sql: `UPDATE envios SET choferAsignado = ?, costoActualizadoChofer = 0 WHERE superado=0 AND elim=0 AND did = ?`, values: [driverId, shipmentId] },
            { sql: `UPDATE ruteo_paradas SET superado = 1 WHERE superado=0 AND elim=0 AND didPaquete = ?`, values: [shipmentId] },
            { sql: `UPDATE envios_historial SET didCadete = ? WHERE superado=0 AND elim=0 AND didEnvio = ?`, values: [driverId, shipmentId] },
        ];

        for (const { sql, values } of queries) {
            await executeQuery(dbConnection, sql, values);
        }
        logCyan("Updateo las tablas");

        await insertAsignacionesDB(dbConnectionLocal, company.did, did, driverId, estado, userId, deviceFrom);
        logCyan("Inserto en la base de datos individual de asignaciones");

        const sendDuration = performance.now() - startTime;
        await updateRedis(company.did, shipmentId, driverId);
        const resultado = { feature: "asignacion", estadoRespuesta: true, mensaje: "Asignación realizada correctamente" };

        crearLog(dbConnectionLocal, company.did, body.userId, body.profile, body, sendDuration.toFixed(2), JSON.stringify(resultado), "asignar", "rabbit", true);

        return resultado;
    } catch (error) {
        const sendDuration = performance.now() - startTime;
        logRed(`Error al asignar paquete:  ${error.stack}`)

        crearLog(dbConnectionLocal, company.did, body.userId, body.profile, body, sendDuration.toFixed(2), error.stack, "asignar", "rabbit", false);

        throw error;
    } finally {
        dbConnectionLocal.end();
    }
}