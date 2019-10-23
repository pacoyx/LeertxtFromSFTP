using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Renci.SshNet;
using System.IO;
using System.Configuration;
using System.Threading;

namespace AppLeerInputs.Transmision
{
    public class TransmisionFtp
    {

        string host = "";
        int puerto = 22;
        string username = "";
        string password = "";
        string carpetaLocal = "";
        string carpetaRemota = "";


        private void LeerConfiguracion_Geo() {

            this.host = ConfigurationManager.AppSettings["host_geo"].ToString();
            this.username = ConfigurationManager.AppSettings["username_geo"].ToString();
            this.puerto = int.Parse(ConfigurationManager.AppSettings["puerto_geo"]);
            this.password = ConfigurationManager.AppSettings["password_geo"].ToString();
            this.carpetaLocal = ConfigurationManager.AppSettings["carpetaLocal_geo"].ToString();
            this.carpetaRemota = ConfigurationManager.AppSettings["carpetaRemota_geo"].ToString();
                    
        }

        public void Download_InputGeoPagos(string filtroComercio, string filtroTrx,string carpeta)
        {
            this.LeerConfiguracion_Geo();

            using (var sftp = new SftpClient(host, puerto, username, password))
            {
                Console.WriteLine("conectando al sftp....");
                sftp.Connect();

                Console.WriteLine("conexion establecida correctamente...");

                Console.WriteLine("Cargando archivos remoto en memoria...");

                var files = sftp.ListDirectory(carpetaRemota);

                Console.WriteLine("Numero de archivos en memoria : {0}", files.Count<object>());

                Console.WriteLine("comenzando proceso...");
                Console.WriteLine("fecha/hora inicio: {0}",DateTime.UtcNow);


                Directory.CreateDirectory(carpetaLocal + carpeta);

                int cont = 0;

                var progress = new ProgressBar();

                foreach (var file in files)
                {                    
                    string nomfile = Path.GetFileName(file.FullName);

                    if (nomfile != "." && nomfile != "..")
                    {
                        if (nomfile.Contains(filtroTrx)) {                            
                            using (var fileD = File.OpenWrite(carpetaLocal + carpeta+ @"\" + filtroTrx + ".txt"))
                            {
                                sftp.DownloadFile(file.FullName, fileD);
                            }
                        }

                        if (nomfile.Contains(filtroComercio))
                        {
                            using (var fileD = File.OpenWrite(carpetaLocal + carpeta + @"\" + nomfile))
                            {
                                sftp.DownloadFile(file.FullName, fileD);
                            }
                        }
                    }


                    cont++;
                    progress.Report((double)cont / files.Count<object>());
                    //Thread.Sleep(20);
                }
                progress = null;

                Console.WriteLine("fecha/hora termino: {0}",DateTime.UtcNow);
                Console.WriteLine("termino proceso...");

                sftp.Disconnect();

                Console.WriteLine("desconectado...");                
            }


        }

        private static void progreso(int progreso, int total = 100) //Default 100
        {
           
            //Dibujar la barra vacia
            Console.CursorLeft = 0;
            Console.Write("["); //inicio
            Console.CursorLeft = 32;
            Console.Write("]"); //fin
            Console.CursorLeft = 1; //Colocar el cursor al inicio
            float onechunk = 30.0f / total;

            //Rellenar la parte indicada
            int position = 1;
            for (int i = 0; i < onechunk * progreso; i++)
            {
                Console.BackgroundColor = ConsoleColor.Green;
                Console.CursorLeft = position++;
                Console.Write(" ");
            }

            //Pintar la otra parte
            for (int i = position; i <= 31; i++)
            {
                Console.BackgroundColor = ConsoleColor.Black;
                Console.CursorLeft = position++;
                Console.Write(" ");
            }

            //Escribir el total al final
            Console.CursorLeft = 35;
            Console.BackgroundColor = ConsoleColor.Black;
            Console.Write(progreso.ToString() + "% de " + total.ToString() + "    ");
        }

    }


}
