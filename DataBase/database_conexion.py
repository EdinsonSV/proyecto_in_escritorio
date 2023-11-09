import mysql.connector
import requests
import json

URLSERVIDOR=""
URLLOCAL = ""

class Conectar():
    def __init__(self):
        # Conexion para MySql
        self.conexionsql = mysql.connector.connect(host='localhost',
                                        user='root',
                                        password='Balinsa2023.',
                                        database='bd_proyectoin',
                                        port='3306')
        
    def db_seleccionaApiURL(self):
        cursor = self.conexionsql.cursor()
        sql = "SELECT puerto_ApiURLSERVIDOR,puerto_ApiURLLOCAL FROM tb_puertos"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result
        
    def db_buscaEspecies(self):
        cursor = self.conexionsql.cursor()
        sql = "SELECT idEspecie,nombreEspecie FROM tb_especies_venta WHERE idEspecie <= 4"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result
    
    def db_seleccionaPuertoIndicadores(self):
        cursor = self.conexionsql.cursor()
        sql = "SELECT puerto_indicador1, puerto_indicador2 FROM tb_puertos"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result
    
    def db_seleccionaPuertoArduino(self):
        cursor = self.conexionsql.cursor()
        sql = "SELECT puerto_indicadorArduino FROM tb_puertos"
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result
    
    def db_seleccionaPuertoHostIp(self):
        cursor = self.conexionsql.cursor()
        sql = "SELECT puerto_HostIP FROM tb_puertos"
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result
    
    def db_declaraPassword(self):
        cursor = self.conexionsql.cursor()
        sql = "SELECT passwordEliminar FROM tb_password"
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result
        
    def db_buscaCliente(self, valor):
        cursor = self.conexionsql.cursor()
        sql = "SELECT IFNULL(CONCAT_WS(' ', nombresCli, apellidoPaternoCli), '') AS nombre_completo, codigoCli, idGrupo, idEstadoCli FROM tb_clientes WHERE estadoEliminadoCli = 1 AND (CONCAT_WS(' ', nombresCli, apellidoPaternoCli) LIKE %s OR codigoCli LIKE %s)"
        cursor.execute(sql, ('%' + valor + '%', '%' + valor + '%'))
        result = cursor.fetchall()
        cursor.close()
        return result
    
    def db_traerPreciosCliente(self,codigoCli):
        cursor = self.conexionsql.cursor()
        sql = "SELECT primerEspecie,segundaEspecie,terceraEspecie,cuartaEspecie,valorConversionPrimerEspecie, valorConversionSegundaEspecie, valorConversionTerceraEspecie, valorConversionCuartaEspecie FROM tb_precio_x_presentacion WHERE codigoCli = %s"
        cursor.execute(sql,(codigoCli,))
        result = cursor.fetchall()
        cursor.close()
        return result
    
    def db_verificarProceso(self,codigoCli):
        cursor = self.conexionsql.cursor()    
        sql = "SELECT idProceso,codigoCli FROM tb_procesos WHERE codigoCli = %s AND fechaInicioPro = DATE(NOW())"
        cursor.execute(sql, (codigoCli,))        
        result = cursor.fetchall()
        cursor.close()
        return result
    
    def db_registrarProceso(self, cliente):
        cursor = self.conexionsql.cursor()
        sql = "INSERT INTO tb_procesos(fechaInicioPro,horaInicioPro,codigoCli) VALUES (DATE(NOW()),TIME(NOW()), %s)"
        cursor.execute(sql,(cliente,))
        self.conexionsql.commit()
        cursor.close()
    
    def db_obtieneUltimoIdProcesoRegistrado(self):
        cursor = self.conexionsql.cursor()
        sql = "SELECT MAX(idProceso) AS idProceso FROM tb_procesos"
        cursor.execute(sql)        
        result = cursor.fetchone()
        cursor.close()
        return result
    
    def db_registrarPesadas(self,numProceso,idEspecie,pesoNeto,horaPeso,codigoCli,fechaPeso,cantidadRegistro,precioCliente,balanzaSeleccionada,numJabas,valorConversion,estadoPeso,estadoWebPeso,observacionPeso,idGrupoCli):
        cursor = self.conexionsql.cursor()
        sql = """INSERT INTO tb_pesadas
                    (idProceso, idEspecie, pesoNetoPes, horaPes, codigoCli, fechaRegistroPes, cantidadPes, precioPes, numBalanzaPes, numeroJabasPes, valorConversion, estadoPes, estadoWebPes, observacionPes,idGrupo) 
                VALUES 
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.execute(sql,(numProceso,idEspecie,pesoNeto,horaPeso,codigoCli,fechaPeso,cantidadRegistro,precioCliente,balanzaSeleccionada,numJabas,valorConversion,estadoPeso,estadoWebPeso,observacionPeso,idGrupoCli))
        self.conexionsql.commit()
        cursor.close()
        
    def db_listarPesosTabla(self,numProceso,codigoCli):
        cursor = self.conexionsql.cursor()
        sql = """SELECT
                    CAST((@rownum:=@rownum-1) as INT) as num,
                    IFNULL(CONCAT_WS(' ', nombresCli, apellidoPaternoCli), '') AS cliente,
                    (SELECT TRUNCATE(pesoNetoPes / cantidadPes, 2) FROM tb_pesadas WHERE idPesada = p.idPesada) AS promedioPesoNetoCantidad,
                    nombreEspecie, TRUNCATE(pesoNetoPes, 2), cantidadPes, horaPes, estadoPes, numeroJabasPes
                FROM
                    (SELECT @rownum:=(SELECT COUNT(idPesada) FROM tb_pesadas WHERE fechaRegistroPes = DATE(NOW()) AND tb_pesadas.codigoCli = %s) + 1) r,
                    tb_pesadas p
                    INNER JOIN tb_clientes ON p.codigoCli = tb_clientes.codigoCli
                    INNER JOIN tb_procesos ON p.idProceso = tb_procesos.idProceso
                    INNER JOIN tb_especies_venta ON p.idEspecie = tb_especies_venta.idEspecie
                WHERE
                    p.fechaRegistroPes = DATE(NOW()) AND p.idProceso = %s AND p.codigoCli = %s
                ORDER BY
                    p.idPesada desc"""
        cursor.execute(sql,(codigoCli, numProceso, codigoCli))
        result = cursor.fetchall()
        cursor.close()
        return result
    
    def db_editarCantidadNueva(self, cantidadNueva, codCliente):
        cursor = self.conexionsql.cursor()
        sql = "UPDATE tb_pesadas SET cantidadPes = %s WHERE codigoCli = %s AND idPesada = (SELECT MAX(idPesada) FROM tb_pesadas WHERE codigoCli = %s)"
        cursor.execute(sql, (cantidadNueva, codCliente, codCliente))
        self.conexionsql.commit()
        cursor.close()
        
    def db_editarCantidadTaraNueva(self, cantidadNueva, codCliente):
        cursor = self.conexionsql.cursor()
        sql = "UPDATE tb_pesadas SET numeroJabasPes = %s WHERE codigoCli = %s AND idPesada = (SELECT MAX(idPesada) FROM tb_pesadas WHERE codigoCli = %s)"
        cursor.execute(sql, (cantidadNueva, codCliente, codCliente))
        self.conexionsql.commit()
        cursor.close()
        
    def db_editarCantidadDescuentoNueva(self, cantidadNueva, codCliente):
        cursor = self.conexionsql.cursor()
        sql = "UPDATE tb_pesadas SET cantidadPes = %s WHERE codigoCli = %s AND idPesada = (SELECT MAX(idPesada) FROM tb_pesadas WHERE codigoCli = %s)"
        cursor.execute(sql, (cantidadNueva, codCliente, codCliente))
        self.conexionsql.commit()
        cursor.close()
        
    def db_eliminarUltimaCantidad(self, codCliente):
        cursor = self.conexionsql.cursor()
        sql = "UPDATE tb_pesadas SET estadoPes = 0 WHERE codigoCli = %s AND idPesada = (SELECT MAX(idPesada) FROM tb_pesadas WHERE codigoCli = %s)"
        cursor.execute(sql, (codCliente, codCliente))
        self.conexionsql.commit()
        cursor.close()
        
    def db_traerDatosReporte(self,numProceso,codigoCli):
        cursor = self.conexionsql.cursor()
        sql = """SELECT
                    nombreEspecie, TRUNCATE(pesoNetoPes, 2), cantidadPes, horaPes,numeroJabasPes
                FROM
                    tb_pesadas p
                    INNER JOIN tb_procesos ON p.idProceso = tb_procesos.idProceso
                    INNER JOIN tb_especies_venta ON p.idEspecie = tb_especies_venta.idEspecie
                WHERE
                    p.fechaRegistroPes = DATE(NOW()) AND p.idProceso = %s AND p.codigoCli = %s AND estadoPes = 1
                ORDER BY
                    p.idPesada asc"""
        cursor.execute(sql,(numProceso, codigoCli))
        result = cursor.fetchall()
        cursor.close()
        return result
    
    def db_verificarProveedor(self,codCliente):
        cursor = self.conexionsql.cursor()
        sql = """SELECT codigoCli FROM tb_clientes WHERE codigoCli = %s"""
        cursor.execute(sql, (codCliente, ))
        result = cursor.fetchone()
        cursor.close()
        return result
    
    def db_verificaridGrupoCli(self,codCliente):
        cursor = self.conexionsql.cursor()
        sql = """SELECT idGrupo FROM tb_clientes WHERE codigoCli = %s"""
        cursor.execute(sql, (codCliente, ))
        result = cursor.fetchone()
        cursor.close()
        return result[0]
    
    def db_verificarPrecios(self,codCliente):
        cursor = self.conexionsql.cursor()
        sql = """SELECT primerEspecie,segundaEspecie,terceraEspecie,cuartaEspecie,valorConversionPrimerEspecie,valorConversionSegundaEspecie,valorConversionTerceraEspecie,valorConversionCuartaEspecie FROM tb_precio_x_presentacion WHERE codigoCli = %s"""
        cursor.execute(sql, (codCliente, ))
        result = cursor.fetchone()
        cursor.close()
        return result
    
    ######################################################################################################
    ###################################### CONSULTAS A SERVIDOR WEB ######################################
    ######################################################################################################
    
    def actualizar_datos_servidor_a_local_precios(self):
            
        datoss = {"query": f"SELECT * FROM tb_precio_x_presentacion"}
        # Realizar la solicitud HTTP POST
        response = requests.post(URLSERVIDOR, json=datoss)

        # Obtener los resultados en formato JSON
        results = json.loads(response.content)
        
        local_data = []
        # Imprimir los resultados
        for row in results:
            local_data.append(list(row.values()))

        # Comparar datos y subir solo los datos que no existen
        for data in local_data:
            try:
                # Actualizar datos de la base de datos local
                cursor = self.conexionsql.cursor()
                query = "UPDATE tb_precio_x_presentacion SET codigoCli = %s, primerEspecie = %s, segundaEspecie = %s, terceraEspecie = %s, cuartaEspecie = %s, valorConversionPrimerEspecie = %s, valorConversionSegundaEspecie = %s, valorConversionTerceraEspecie = %s, valorConversionCuartaEspecie = %s WHERE idPrecio = %s"
                cursor.execute(query, (data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[0]))
                self.conexionsql.commit()
                query2 = "INSERT IGNORE INTO tb_precio_x_presentacion (idPrecio, codigoCli, primerEspecie, segundaEspecie, terceraEspecie, cuartaEspecie, valorConversionPrimerEspecie, valorConversionSegundaEspecie, valorConversionTerceraEspecie, valorConversionCuartaEspecie) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(query2, (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9]))
                self.conexionsql.commit()
            except requests.exceptions.RequestException as e:
                print(e)
            finally:
                # Cerrar la conexión a la base de datos
                cursor.close()
    
    def actualizar_datos_servidor_a_local_clientes(self):
        
        datoss = {"query": f"SELECT * FROM tb_clientes"}
        # Realizar la solicitud HTTP POST

        response = requests.post(URLSERVIDOR, json=datoss)
        # Obtener los resultados en formato JSON
        results = json.loads(response.content)
        
        local_data = []
        # Imprimir los resultados
        for row in results:
            local_data.append(list(row.values()))

        # Comparar datos y subir solo los datos que no existen
        for data in local_data:
            try:
                # Actualizar datos de la base de datos local
                cursor = self.conexionsql.cursor()
                query2 = "UPDATE tb_clientes SET apellidoPaternoCli = %s, apellidoMaternoCli = %s, nombresCli = %s, tipoDocumentoCli = %s, numDocumentoCli = %s, contactoCli = %s, direccionCli = %s, idEstadoCli = %s, fechaRegistroCli = %s, horaRegistroCli = %s, usuarioRegistroCli = %s, codigoCli = %s, idGrupo = %s, comentarioCli = %s, idZona = %s, estadoEliminadoCli = %s WHERE idCliente = %s"
                cursor.execute(query2, (data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16], data[0]))
                self.conexionsql.commit()
                query = "INSERT IGNORE INTO tb_clientes (idCliente, apellidoPaternoCli, apellidoMaternoCli, nombresCli, tipoDocumentoCli, numDocumentoCli, contactoCli, direccionCli, idEstadoCli, fechaRegistroCli, horaRegistroCli, usuarioRegistroCli, codigoCli, idGrupo, comentarioCli, idZona, estadoEliminadoCli) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16]))
                self.conexionsql.commit()
            except requests.exceptions.RequestException as e:
                print(e)
            finally:
                # Cerrar la conexión a la base de datos
                cursor.close()
                
    def actualizar_datos_servidor_a_local_password(self):
        
        datoss = {"query": f"SELECT idPassword, passwordEliminar FROM tb_password"}
        # Realizar la solicitud HTTP POST

        response = requests.post(URLSERVIDOR, json=datoss)
        # Obtener los resultados en formato JSON
        results = json.loads(response.content)
        
        local_data = []
        # Imprimir los resultados
        for row in results:
            local_data.append(list(row.values()))

        # Comparar datos y subir solo los datos que no existen
        for data in local_data:
            try:
                # Actualizar datos de la base de datos local
                cursor = self.conexionsql.cursor()
                query2 = "UPDATE tb_password SET passwordEliminar = %s WHERE idPassword = %s"
                cursor.execute(query2, (data[1], data[0]))
                self.conexionsql.commit()
                query = "INSERT IGNORE INTO tb_password (idPassword, passwordEliminar) VALUES (%s, %s)"
                cursor.execute(query, (data[0], data[1]))
                self.conexionsql.commit()
            except requests.exceptions.RequestException as e:
                print(e)
            finally:
                # Cerrar la conexión a la base de datos
                cursor.close()
        
    def actualizar_datos_servidor_pesadas(self):
        
        # Obtener datos de la base de datos local
        cursor = self.conexionsql.cursor()
        query = f"SELECT * FROM tb_pesadas WHERE fechaRegistroPes = DATE(NOW())"
        cursor.execute(query)
        local_data = cursor.fetchall()

        # Comparar datos y subir solo los datos que no existen
        for data in local_data:
            try:
                # Realizar la solicitud HTTP POST
                datos = {"query": f"INSERT IGNORE INTO tb_pesadas (idPesada, idProceso, idEspecie, pesoNetoPes, horaPes, codigoCli, fechaRegistroPes, cantidadPes, precioPes, numBalanzaPes, numeroJabasPes, valorConversion, estadoPes, estadoWebPes, observacionPes, idGrupo) VALUES ({data[0]},{data[1]},{data[2]},{data[3]},'{data[4]}',{data[5]},'{data[6]}',{data[7]},{data[8]},{data[9]},{data[10]},{data[11]},{data[12]},{data[13]},'{data[14]}',{data[15]})"}
                requests.post(URLLOCAL, json=datos)
                # Realizar la solicitud HTTP POST
                datos = {"query": f"UPDATE tb_pesadas SET cantidadPes = {data[7]} , estadoPes = {data[12]} WHERE idPesada = {data[0]} AND estadoWebPes = 1"}

                requests.post(URLLOCAL, json=datos)
            except requests.exceptions.RequestException as e:
                print(e)
        # Cerrar la conexión a la base de datos
        cursor.close()
    
    def actualizar_datos_servidor_procesos(self):
        
        # Obtener datos de la base de datos local
        cursor = self.conexionsql.cursor()
        query = f"SELECT * FROM tb_procesos WHERE fechaInicioPro = DATE(NOW())"
        cursor.execute(query)
        local_data = cursor.fetchall()
        
        # Comparar datos y subir solo los datos que no existen
        for data in local_data:
            try:
                datoss = {"query": f"INSERT IGNORE INTO tb_procesos (idProceso, fechaInicioPro, horaInicioPro, codigoCli) VALUES ({data[0]},'{data[1]}','{data[2]}',{data[3]})"}
                # Realizar la solicitud HTTP POST
                requests.post(URLLOCAL, json=datoss)
            except requests.exceptions.RequestException as e:
                print(e)
        
        # Cerrar la conexión a la base de datos
        cursor.close()
        
# DISEÑADO Y DESARROLLADO POR SANTOS VILCHEZ EDINSON PASCUAL
# LA UNIÓN - PIURA - PERU ; 2023