using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AppLeerInputs.Transmision;
using AppLeerInputs.Operaciones;

namespace AppLeerInputs
{
    class Program
    {
        static void Main(string[] args)
        {
            //importamos los txt de geopagos
            Input_Geopagos();
        }

       static void Input_Geopagos() {
            Console.WriteLine("Inicio de proceso archivos input geopagos");

            string filtroCom = "";
            string filtroTrx = "";
            Console.WriteLine("Ingrese fecha(yyyyMMdd) para descargar Transacciones y Comercios:");
            string fechaProceso = Console.ReadLine();
            filtroTrx = "transacciones_" + fechaProceso;
            filtroCom = "comercios_" + fechaProceso;
          
            TransmisionFtp tra = new TransmisionFtp();
            tra.Download_InputGeoPagos(filtroCom, filtroTrx, fechaProceso);
            tra = null;
           
            OperacionesTrans ope = new OperacionesTrans();
            ope.Grabar_InputGeoComercio(filtroTrx, filtroCom, fechaProceso);
            ope = null;

            Console.WriteLine("");
            Console.WriteLine("Termino de procesar archivos input geopagos");
            Console.ReadLine();

        }
               
    }
}
