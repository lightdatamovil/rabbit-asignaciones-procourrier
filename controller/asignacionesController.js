import { executeQuery, getDbConfig, getProdDbConfig } from '../db.js';
import mysql from 'mysql';

export async function verificacionDeAsignacion(company, userId, profile, dataQr, driverId, deviceFrom) {
    const dbConfig = getProdDbConfig(company);
    const dbConnection = mysql.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const isFlex = dataQr.hasOwnProperty("sender_id");

        const shipmentId = isFlex
            ? await idFromFlexShipment(dataQr.id, dbConnection)
            : await idFromLightdataShipment(company, dataQr, dbConnection);

        let hoy = new Date();
        hoy.setDate(hoy.getDate() - 3);
        hoy = hoy.toISOString().split('T')[0];

        let sql = `
                SELECT e.did, e.quien, sua.perfil, e.autofecha, e.estadoAsignacion
                FROM envios AS e
                LEFT JOIN sistema_usuarios_accesos AS sua 
                    ON sua.usuario = e.quien AND sua.superado = 0 AND sua.elim = 0
                WHERE e.did = ${shipmentId} AND e.superado = 0 AND e.elim = 0 AND e.autofecha > ${hoy}
                ORDER BY e.autofecha ASC
            `;

        const envios = await executeQuery(dbConnection, sql, []);

        if (envios.length === 0) {
            return { estadoRespuesta: false, mensaje: "No se encontró el paquete." };
        }

        const envio = envios[0];

        let ponerEnEstado1 = false;
        let ponerEnEstado2 = false;
        let ponerEnEstado3 = false;
        let ponerEnEstado4 = false;
        let ponerEnEstado5 = false;

        let estadoAsignacion = envio.estadoAsignacion;

        let resultHistorial = await executeQuery(dbConnection,
            "SELECT estado, didCadete FROM envios_historial WHERE didEnvio = ? AND superado = 0 LIMIT 1",
            [shipmentId]
        );

        let didCadete = resultHistorial.length > 0 ? resultHistorial[0].didCadete : null;
        let esElMismoCadete = didCadete === driverId;

        if (esElMismoCadete) {
            if (profile === 1 && estadoAsignacion === 1) {
                return { estadoRespuesta: false, mensaje: "Este paquete ya fue asignado a este cadete" };
            }
            if (profile === 3 && estadoAsignacion === 2) {
                return { estadoRespuesta: false, mensaje: "Este paquete ya fue auto asignado a este cadete" };
            }
            if (profile === 5 && [3, 4, 5].includes(estadoAsignacion)) {
                return { estadoRespuesta: false, mensaje: "Este paquete ya fue confirmado" };
            }
        } else {
            if (profile === 1 && estadoAsignacion === 1) {
                return { estadoRespuesta: false, mensaje: "Este paquete ya fue asignado a otro cadete" };
            }
            if (profile === 3 && estadoAsignacion === 2) {
                return { estadoRespuesta: false, mensaje: "Este paquete ya fue auto asignado por otro cadete" };
            }
            if (profile === 5 && [1, 3, 4, 5].includes(estadoAsignacion)) {
                return { estadoRespuesta: false, mensaje: "Este paquete ya fue confirmado o asignado a otro cadete" };
            }
        }

        if (profile === 1 && estadoAsignacion === 0) ponerEnEstado1 = true;
        if (profile === 3 && estadoAsignacion === 1) ponerEnEstado2 = true;
        if (profile === 5 && estadoAsignacion === 0) ponerEnEstado5 = true;
        if (profile === 5 && estadoAsignacion === 1) ponerEnEstado4 = true;
        if (profile === 5 && estadoAsignacion === 2) ponerEnEstado3 = true;


        let noCumple = false;
        let mensaje = "No se puede asignar el paquete.";

        if (ponerEnEstado1) {
            await executeQuery(dbConnection, "UPDATE envios SET estadoAsignacion = 1 WHERE superado = 0 AND elim = 0 AND did = ?", [shipmentId]);
            mensaje = "Asignado correctamente.";
        } else if (ponerEnEstado2) {
            await executeQuery(dbConnection, "UPDATE envios SET estadoAsignacion = 2 WHERE superado = 0 AND elim = 0 AND did = ?", [shipmentId]);
            mensaje = "Autoasignado correctamente.";
        } else if (ponerEnEstado3) {
            await executeQuery(dbConnection, "UPDATE envios SET estadoAsignacion = 3 WHERE superado = 0 AND elim = 0 AND did = ?", [shipmentId]);
            mensaje = "Confirmado correctamente.";
        } else if (ponerEnEstado4) {
            await executeQuery(dbConnection, "UPDATE envios SET estadoAsignacion = 4 WHERE superado = 0 AND elim = 0 AND did = ?", [shipmentId]);
            mensaje = "Confirmado correctamente.";
        } else if (ponerEnEstado5) {
            await executeQuery(dbConnection, "UPDATE envios SET estadoAsignacion = 5 WHERE superado = 0 AND elim = 0 AND did = ?", [shipmentId]);
            mensaje = "Asignado correctamente.";
        } else {
            noCumple = true;
        }

        if (noCumple) {
            return { estadoRespuesta: false, mensaje };
        } else {
            await asignar(dbConnection, company, userId, driverId, deviceFrom, shipmentId);
            return { estadoRespuesta: true, mensaje };
        }
    } catch (error) {
        console.error("Error al verificar la asignación:", error);
        throw error;
    }
}

async function asignar(dbConnection, company, userId, driverId, deviceFrom, shipmentId) {
    try {
        const sqlAsignado = `SELECT id, estado FROM envios_asignaciones WHERE superado=0 AND elim=0 AND didEnvio = ? AND operador = ?`;
        const asignadoRows = await executeQuery(dbConnection, sqlAsignado, [shipmentId, driverId]);

        if (asignadoRows.length > 0) {
            return { success: false, mensaje: "El paquete ya se encuentra asignado a este chofer." };
        }

        const estadoQuery = `SELECT estado FROM envios_historial WHERE superado=0 AND elim=0 AND didEnvio = ?`;

        const estadoRows = await executeQuery(dbConnection, estadoQuery, [shipmentId]);

        if (estadoRows.length === 0) {
            throw new Error("No se pudo obtener el estado del paquete.");
        }

        const estado = estadoRows[0].estado;

        await crearTablaAsignaciones(company.did, dbConnection);
        await crearUsuario(company.did, dbConnection);

        const insertSql = `INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES (?, ?, ?, ?, ?, ?)`;

        const result = await executeQuery(dbConnection, insertSql, ["", driverId, shipmentId, estado, userId, deviceFrom]);

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

        await insertAsignacionesDB(company.did, did, driverId, estado, userId, deviceFrom);

        return { success: true, mensaje: "Asignación realizada correctamente" };
    } catch (error) {
        console.error("Error al asignar paquete:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

export async function desasignar(company, userId, dataQr, deviceFrom) {
    const dbConfig = getProdDbConfig(company);
    const dbConnection = mysql.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const isFlex = dataQr.hasOwnProperty("sender_id");

        const shipmentId = isFlex
            ? await idFromFlexShipment(dataQr.id, dbConnection)
            : await idFromLightdataShipment(company, dataQr, dbConnection);

        const sqlOperador = "SELECT operador FROM envios_asignaciones WHERE didEnvio = ? AND superado = 0 AND elim = 0";

        const result = await executeQuery(dbConnection, sqlOperador, [shipmentId]);

        const operador = result.length > 0 ? result[0].operador : 0;

        if (operador == 0) {
            return { success: false, mensaje: "El paquete ya está desasignado" };
        }

        if (!shipmentId) {
            throw new Error("No se pudo obtener el id del envío.");
        }

        const setEstadoAsignacion = "UPDATE envios SET estadoAsignacion = 0 WHERE superado=0 AND elim=0 AND did = ?";
        await executeQuery(dbConnection, setEstadoAsignacion, [shipmentId]);

        const sq = "SELECT estado FROM `envios_historial` WHERE  didEnvio = ? and superado=0 LIMIT 1";
        const estado = await executeQuery(dbConnection, sq, [shipmentId]);

        const insertQuery = "INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES (?, ?, ?, ?, ?, ?)";
        const resultInsertQuery = await executeQuery(dbConnection, insertQuery, ["", 0, shipmentId, estado[0].estado, userId, deviceFrom]);

        // Actualizar asignaciones
        await executeQuery(dbConnection, `UPDATE envios_asignaciones SET superado=1, did=${resultInsertQuery.insertId} WHERE superado=0 AND elim=0 AND didEnvio = ?`, [shipmentId]);

        // Actualizar historial
        await executeQuery(dbConnection, `UPDATE envios_historial SET didCadete=0 WHERE superado=0 AND elim=0 AND didEnvio = ?`, [shipmentId]);

        // Desasignar chofer
        await executeQuery(dbConnection, `UPDATE envios SET choferAsignado = 0 WHERE superado=0 AND elim=0 AND did = ?`, [shipmentId]);

        return { success: true, mensaje: "Desasignación realizada correctamente" };
    } catch (error) {
        console.error("Error al desasignar paquete:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

async function idFromLightdataShipment(company, dataQr, dbConnection) {
    const companyIdFromShipment = dataQr.empresa;

    const shipmentId = dataQr.did;

    if (company.did != companyIdFromShipment) {
        try {
            const sql = `SELECT didLocal FROM envios_exteriores WHERE superado=0 AND elim=0 AND didExterno = ? AND didEmpresa = ?`;
            const rows = await executeQuery(dbConnection, sql, [companyIdFromShipment, companyIdFromShipment]);

            if (rows.length > 0) {
                shipmentId = rows[0]["didLocal"];
                return shipmentId;
            } else {
                throw new Error("El paquete externo no existe en la logística.");
            }
        } catch (error) {
            console.error("Error al obtener el id del envío:", error);
            throw error;
        }
    } else {
        return shipmentId;
    }
}

async function idFromFlexShipment(shipmentId, dbConnection) {
    try {
        const query = `SELECT did FROM envios WHERE flex=1 AND superado=0 AND elim=0 AND ml_shipment_id = ?`;
        const rows = await executeQuery(dbConnection, query, [shipmentId]);

        if (rows.length > 0) {
            const didenvio = rows[0].did;
            return didenvio;
        } else {
            throw new Error("El paquete flex no se encontró en la base de datos.");
        }
    } catch (error) {
        console.error("Error al obtener el id del envío:", error);
        throw error;
    }
}

async function crearTablaAsignaciones(companyId) {
    const dbConfig = getDbConfig();
    const dbConnection = mysql.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const createTableSql = `
            CREATE TABLE IF NOT EXISTS asignaciones_${companyId} (
                id INT NOT NULL AUTO_INCREMENT,
                didenvio INT NOT NULL,
                chofer INT NOT NULL,
                estado INT NOT NULL DEFAULT 0,
                quien INT NOT NULL,
                autofecha TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                desde INT NOT NULL COMMENT '0 = asignacion / 1 = web',
                superado INT NOT NULL DEFAULT 0,
                elim INT NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                KEY didenvio (didenvio),
                KEY chofer (chofer),
                KEY autofecha (autofecha)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        `;

        await executeQuery(dbConnection, createTableSql);
    } catch (error) {
        console.error("Error al crear la tabla de asignaciones:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

async function crearUsuario(companyId) {
    const dbConfig = getDbConfig();
    const dbConnection = mysql.createConnection(dbConfig);
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
        console.error("Error al crear el usuario:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

async function insertAsignacionesDB(companyId, shipmentId, driverId, shipmentState, userId, deviceFrom) {
    const dbConfig = getDbConfig();
    const dbConnection = mysql.createConnection(dbConfig);
    dbConnection.connect();

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
        console.error("Error al insertar asignaciones en la base de datos:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}
