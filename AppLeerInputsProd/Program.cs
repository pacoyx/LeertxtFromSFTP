using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AppLeerInputs.Transmision;
using AppLeerInputs.Operaciones;
using NLog;

namespace AppLeerInputs
{
    class Program
    {
           


        static void Main(string[] args)
        {
            //obtenemos el IDProceso
            int idproceso = 1522;

            //importamos los txt de geopagos          
            Input_Geopagos(idproceso);

            //importamos los txt NBO visa

            //importamos los txt PMC

            //importamos los txt de amex

            //importamos los txt de diner
        }

        static void Input_Geopagos(int idproceso) {

            Logger loggerx = LogManager.GetCurrentClassLogger();

            //LogEventInfo theEvent = new LogEventInfo(LogLevel.Info,loggerx.Name,  "Pass my custom value");
            //theEvent.Properties["IdProceso"] = "1001";
            //loggerx.Log(theEvent);



            Console.WriteLine("{0}|Inicio de proceso archivos input geopagos", idproceso);
            loggerx.Info(idproceso + "|Inicio de proceso archivos input geopagos");

            string filtroCom = "";
            string filtroTrx = "";
            Console.WriteLine("Ingrese fecha(yyyyMMdd) para descargar Transacciones y Comercios:");
            string fechaProceso = Console.ReadLine();
            filtroTrx = "transacciones_" + fechaProceso;
            filtroCom = "comercios_" + fechaProceso;

            TransmisionFtp tra = new TransmisionFtp();
            tra.Download_InputGeoPagos(idproceso,filtroCom, filtroTrx, fechaProceso);
            tra = null;

            OperacionesTrans ope = new OperacionesTrans(idproceso);
            ope.Grabar_InputGeoComercio(idproceso,filtroTrx, filtroCom, fechaProceso);
            ope = null;

            Console.WriteLine("");
            Console.WriteLine("Termino de procesar archivos input geopagos");
            loggerx.Info(idproceso + "|Termino de procesar archivos input geopagos");         

        }
               
    }
}
