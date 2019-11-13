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
            //ingresamos la fecha de proceso
            Console.WriteLine("Ingrese fecha(yyyyMMdd) para descargar Transacciones y Comercios:");
            string fechaProceso = Console.ReadLine();


            //obtenemos el IDProceso
            //var random = new System.Random();
            //int idproceso = random.Next();
            int idproceso = 0;

            OperacionesTrans clsOpe = new OperacionesTrans();
            idproceso = clsOpe.SQL_Crea_Obtiene_IdProceso("robot charles");
            clsOpe = null;

            if (idproceso == 0)
            {
                return;
            }


            //importamos los txt de geopagos          
            Input_Geopagos(idproceso, fechaProceso);

            //importamos los txt NBO visa

            //importamos los txt PMC
            Input_Pmc(idproceso, fechaProceso);

            //importamos los txt de amex

            //importamos los txt de diner
        }

        static void Input_Geopagos(int idproceso,string fechaProceso) {

            Logger loggerx = LogManager.GetCurrentClassLogger();

            Console.WriteLine("{0}|Inicio de proceso archivos input geopagos", idproceso);
            loggerx.Info(idproceso + "|Inicio de proceso archivos input geopagos");

            string filtroCom = "";
            string filtroTrx = "";
           
            filtroTrx = "transacciones_" + fechaProceso;
            filtroCom = "comercios_" + fechaProceso;

            TransmisionFtp tra = new TransmisionFtp();
            tra.Download_InputGeoPagos(idproceso, filtroCom, filtroTrx, fechaProceso);
            tra = null;

            OperacionesTrans ope = new OperacionesTrans(idproceso);
            ope.Grabar_InputGeoPagos(idproceso, filtroTrx, fechaProceso);          
            ope = null;

                    

            Console.WriteLine("");
            Console.WriteLine("Termino de procesar archivos input geopagos");
            loggerx.Info(idproceso + "|Termino de procesar archivos input geopagos");         

        }

        static void Input_NBO(int idproceso,string fechaProceso)
        {
            Logger loggerx = LogManager.GetCurrentClassLogger();

            Console.WriteLine("{0}|Inicio de proceso archivos input NBO", idproceso);
            loggerx.Info(idproceso + "|Inicio de proceso archivos input NBO");
           

            TransmisionFtp tra = new TransmisionFtp();
            tra.Download_Input_NBO(idproceso, fechaProceso);
            tra = null;

            OperacionesTrans ope = new OperacionesTrans(idproceso);
            ope.Grabar_Input_NBO(idproceso, fechaProceso);
            ope = null;

            Console.WriteLine("");
            Console.WriteLine("Termino de procesar archivos input NBO");
            loggerx.Info(idproceso + "|Termino de procesar archivos input NBO");

        }

        static void Input_Pmc(int idproceso,string fechaProceso)
        {
            Logger loggerx = LogManager.GetCurrentClassLogger();

            Console.WriteLine("{0}|Inicio de proceso archivos input pmc", idproceso);
            loggerx.Info(idproceso + "|Inicio de proceso archivos input pmc");          
                    

            TransmisionFtp tra = new TransmisionFtp();
            tra.Download_InputPMC(idproceso, fechaProceso);
            tra = null;

            OperacionesTrans ope = new OperacionesTrans(idproceso);
            ope.Grabar_InputPMC(idproceso, fechaProceso);
            ope = null;

            Console.WriteLine("");
            Console.WriteLine("Termino de procesar archivos input PMC");
            loggerx.Info(idproceso + "|Termino de procesar archivos input PMC");
            Console.ReadLine();
        }
    }
}
