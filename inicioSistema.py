from View.Ui_inicioSistema import Ui_MainWindow
from PyQt5.QtCore import *
from PyQt5.QtGui import *      
from PyQt5 import QtCore
from PyQt5.QtWidgets import QApplication, QMainWindow
import sys
import sistemaVentas
import serial

# Importación de Base de Datos
import DataBase.database_conexion # El archivo database_conexion.py

COM1 = ""
COM2 = ""

"""Creamos hilo para la ejecución en segundo plano del Indicador , de esta forma
evitamos que la aplicación se detenga por la lectura constante """

class WorkerThread(QThread):
    update_peso = pyqtSignal(str)
    update_estado = pyqtSignal(str)
    update_baliza = pyqtSignal(str)
    def run(self):
        try:
            COMINDICADOR1 = "COM"+ COM1
            serialIndicador = serial.Serial(COMINDICADOR1, baudrate=9600, timeout=1)
            
            while True:
                result = serialIndicador.readline().decode('utf-8')
                if not result:
                    self.update_peso.emit("0.00")
                    self.update_estado.emit("0")   
                else:
                    #self.update_peso.emit(result[6:14])
                    self.update_peso.emit(result[2:10])
                    self.update_baliza.emit(result[2:10])
                    self.update_estado.emit("1")
        except Exception as e:
            print("WT IN"+str(e))
    
    def stop(self):
        print("Thread Stopped")
        self.terminate() 

"""Creamos hilo para la ejecución en segundo plano del Indicador , de esta forma
evitamos que la aplicación se detenga por la lectura constante """

class WorkerThread2(QThread):
    update_peso2 = pyqtSignal(str)
    update_estado2 = pyqtSignal(str)
    update_baliza2 = pyqtSignal(str)
    def run(self):
        try:
            COMINDICADOR2 = "COM"+ COM2
            serialIndicador2 = serial.Serial(COMINDICADOR2, baudrate=9600, timeout=1)
            
            while True:
                result2 = serialIndicador2.readline().decode('utf-8')
                if not result2:
                    self.update_peso2.emit("0.00")
                    self.update_estado2.emit("0")   
                else:
                    #self.update_peso2.emit(result2[6:14])
                    self.update_peso2.emit(result2[2:10])
                    self.update_baliza2.emit(result2[2:10])
                    self.update_estado2.emit("1")
        except Exception as e:
            print("WT IN2"+str(e))  
    
    def stop(self):
        print("Thread Stopped")
        self.terminate()   

# ===============================
# Creación de la Clase Principal
# ===============================

class InicioSistema(QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.conexion = DataBase.database_conexion.Conectar()
        
        self.moduloVentas = sistemaVentas.Inicio()
        
        self.ui.setupUi(self)
        self.setWindowFlag(QtCore.Qt.FramelessWindowHint)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground)
        
        self.ui.imgVenta.setPixmap(QPixmap("Imagenes/icon_venta.png"))
        self.ui.lblmimizar.setPixmap(QPixmap("Imagenes/minimizar.png"))
        self.ui.lblcerrar.setPixmap(QPixmap("Imagenes/cerrar.png"))
        
        self.ui.btnCerrar.clicked.connect(self.fn_cerrarPrograma)
        self.ui.btnminimizar.clicked.connect(self.fn_minimizarPrograma)

        
        self.fn_declararPuertoIndicadores()
        
        self.worker = WorkerThread() # Hilo Balanza 1
        self.worker.start()
        self.worker.update_peso.connect(self.moduloVentas.evt_actualizar_peso)
        self.worker.update_estado.connect(self.moduloVentas.evt_actualizar_estado)
        self.worker.update_baliza.connect(self.moduloVentas.evt_actualizar_baliza)
        
        self.worker2 = WorkerThread2() # Hilo Balanza 2
        self.worker2.start()
        self.worker2.update_peso2.connect(self.moduloVentas.evt_actualizar_peso2)
        self.worker2.update_estado2.connect(self.moduloVentas.evt_actualizar_estado2)
        self.worker2.update_baliza2.connect(self.moduloVentas.evt_actualizar_baliza2)
    
    def fn_declararPuertoIndicadores(self):
        global COM1
        global COM2
        
        puertoIndicadores = self.conexion.db_seleccionaPuertoIndicadores()
        COM1 = str(puertoIndicadores[0][0])
        COM2 = str(puertoIndicadores[0][1])
        
    def fn_cerrarPrograma(self):
        self.close()
    
    def fn_minimizarPrograma(self):
        self.showMinimized()

    # ======================== Eventos con el Teclado ========================
    
    def keyPressEvent(self, event):       
              
            if event.key() == Qt.Key_Enter or event.key() == Qt.Key_Return:
                if not self.moduloVentas:
                    self.moduloVentas = sistemaVentas.Inicio()
                elif not self.moduloVentas.isVisible():
                    self.moduloVentas.show()
                else:
                    self.moduloVentas.showNormal()
                    self.moduloVentas.activateWindow()
    
    # ======================== Termina eventos con el Teclado ========================
    
if __name__ == '__main__':
    app = QApplication(sys.argv)
    gui = InicioSistema()
    gui.show()
    sys.exit(app.exec_())
    
# DISEÑADO Y DESARROLLADO POR SANTOS VILCHEZ EDINSON PASCUAL
# LA UNIÓN - PIURA - PERU ; 2023