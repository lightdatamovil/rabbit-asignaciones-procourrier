import { executeQuery } from "../../db.js";
import { logRed } from "../../src/funciones/logsCustom.js";


export async function insertAsignacionesDB(dbConnection, companyId, shipmentId, driverId, shipmentState, userId, deviceFrom) {
    try {
        const checkSql = `SELECT id FROM asignaciones_${companyId} WHERE didenvio = ? AND superado = 0`;
        const existingRecords = await executeQuery(dbConnection, checkSql, [shipmentId]);

        if (existingRecords.length > 0) {
            const updateSql = `UPDATE asignaciones_${companyId} SET superado = 1 WHERE id = ?`;
            await executeQuery(dbConnection, updateSql, [existingRecords[0].id]);

            const insertSql = `INSERT INTO asignaciones_${companyId} (didenvio, chofer, estado, quien, desde) VALUES (?, ?, ?, ?, ?)`;
            await executeQuery(dbConnection, insertSql, [shipmentId, driverId, shipmentState, userId, deviceFrom]);
        } else {
            const insertSql = `INSERT INTO asignaciones_${companyId} (didenvio, chofer, estado, quien, desde) VALUES (?, ?, ?, ?, ?)`;
            await executeQuery(dbConnection, insertSql, [shipmentId, driverId, shipmentState, userId, deviceFrom]);
        }
    } catch (error) {
        logRed(`Error al insertar asignaciones en la base de datos:  ${error.stack}`)

        throw error;
    }
}
