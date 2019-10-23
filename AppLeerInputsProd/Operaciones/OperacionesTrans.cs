using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using AppLeerInputs.Models;
using System.IO;
using System.Configuration;
using System.Threading;

namespace AppLeerInputs.Operaciones
{
    public class OperacionesTrans
    {
        string carpetaLocal = "";
        string serverBD = "";
        string nombreBD = "";
        string usuarioSQL = "";
        string pwdSQL = "";
        
        public OperacionesTrans() {
            LeerConfiguracion_Geo();
        }
        
        private void LeerConfiguracion_Geo()
        {
            this.carpetaLocal = ConfigurationManager.AppSettings["carpetaLocal_geo"].ToString();
            this.serverBD = ConfigurationManager.AppSettings["serverBD_xzedi"].ToString();
            this.nombreBD = ConfigurationManager.AppSettings["nombreBD_xzedi"].ToString();
            this.usuarioSQL = ConfigurationManager.AppSettings["usuarioSQL_xzedi"].ToString();
            this.pwdSQL = ConfigurationManager.AppSettings["pwdSQL_xzedi"].ToString();
        }

        public void Grabar_InputGeoComercio(string nomArchivoTRX, string nomArchivoCOM,string carpeta)
        {
            string rutaArchivo = this.carpetaLocal + carpeta + @"\" + nomArchivoTRX + ".txt";
            int IdProceso = 100;
            string line;
            List<In_gp_trx_txtBE> listaENT = new List<In_gp_trx_txtBE>();
            List<In_gp_comercios_txtBE> listaComercios = new List<In_gp_comercios_txtBE>();

            Console.WriteLine("fecha/hora inicio SQL: {0}", DateTime.UtcNow);

            //leemos el archivo txt de transacciones GeoPagos
            StreamReader file = new StreamReader(rutaArchivo);
            while ((line = file.ReadLine()) != null)
            {
                var fila = line.Split('\t');
                In_gp_trx_txtBE ent = new In_gp_trx_txtBE()
                {
                    IdProceso = IdProceso,
                    TipodeTransaccion = fila[0],
                    FechaTransaccion = fila[1],
                    CodigoMCC = fila[2],
                    Actividad = fila[3],
                    TerminalID = fila[4],
                    IDgeopagos = fila[5],
                    CodigodeComercio = fila[6],
                    CodigoUsuario = fila[7],
                    NombreUsuario = fila[8],
                    CorreoUsuario = fila[9],
                    NombredeComercio = fila[10],
                    CodigodeLocal = fila[11],
                    NombredeLocal = fila[12],
                    Cuentadeabono = fila[13],
                    Tipodedocumentodeabono = fila[14],
                    Documentodeabono = fila[15],
                    Nombredeabono = fila[16],
                    TipodePago = fila[17],
                    NombredeBanco = fila[18],
                    NdecuentabancariaNTarjeta = fila[19],
                    CuentaNBO = fila[20],
                    Numerodecuentainterbancaria = fila[21],
                    Tipodecuentabancaria = fila[22],
                    Tipodetarjeta = fila[23],
                    RepresentanteLegal = fila[24],
                    Estado = fila[25],
                    Descripcionestado = fila[26],
                    RespuestaVisa = fila[27],
                    CorreoComprador = fila[28],
                    NombreComprador = fila[29],
                    Marcadetarjeta = fila[30],
                    Numerodetarjeta = fila[31],
                    Geolocalizacion = fila[32],
                    Moneda = fila[33],
                    TransactionID = fila[34],
                    Codigodeoperacion = fila[35],
                    Nderefetencia = fila[36],
                    Ndeautorizacion = fila[37],
                    Ndesecuencia = fila[38],
                    Montotransaccion = fila[39],
                    Tipodecampana = fila[40],
                    TipoComision = fila[41],
                    PorcentajeComision = fila[42],
                    ImportedecomisionVendeMas = fila[43],
                    ImporteIGVdecomisionVendeMas = fila[44],
                    Importenetoadepositar = fila[45],
                    Dispositivo = fila[46],
                    PorcentajedelacomisionVisanet = fila[47],
                    MontodelacomisionVisanet = fila[48],
                    ImpuestodelacomisionVisanet = fila[49],
                    Comisionaldescargar = fila[50],
                    CodigoNBO = fila[51],
                    AlianzaComercial = fila[52],
                    Estadomonitoreo = fila[53],
                    CodigoRecargasServicios = fila[54],
                    CodigoMC = fila[55],
                    Fuente = fila[56],
                };
                listaENT.Add(ent);
            };



            //leemos el archivo txt de comercios(2xmin) GeoPagos
            string[] files = Directory.GetFiles(this.carpetaLocal + carpeta);
            int counter = 0;
            int counterF = 0;
         
            foreach (var item in files)
            {

                if (!item.Contains(nomArchivoTRX))
                {
                    StreamReader fileC = new StreamReader(item);
                    while ((line = fileC.ReadLine()) != null)
                    {
                        var fila = line.Split('\t');
                        In_gp_comercios_txtBE comerENT = new In_gp_comercios_txtBE() {

                            IdProceso = IdProceso,
                            Codigopadre = fila[0],
                            Estado = fila[1],
                            Ruccomerciopatrocinado = fila[2],
                            IDgeopagos = fila[3],
                            RazonSocial = fila[4],
                            Codigo = fila[5],
                            Nombrecomercio = fila[6],
                            Representantelegal = fila[7],
                            NumerodeDocumento = fila[8],
                            EmailComercial = fila[9],
                            UsuarioGrupo = "",
                            CodigoNBO = fila[11],
                            DireccionComercial = fila[12],
                            Distrito = fila[13],
                            Provincia = fila[14],
                            Deparmaneto = fila[15],

                            Telefono1 = fila[10],
                            UsuarioNombre ="",    
                            UsuarioApellido = "",
                            UsuarioCorreo = ""                                                                                                ,
                            Nrodocumentovendedor = fila[8],
                            Telefono2 = fila[10],
                            CantidaddemPOSComercio = "",
                            Serialesmpos = "",

                            MCC = fila[16],
                            DescripcionMCC = fila[17],
                            TipoComision = fila[18],
                            ComisionCredito = fila[19],
                            ComisionDebito = fila[20],
                            Fechadealtacomerciopatrociando = fila[21],
                            Fechadebajacomerciopatrocinado = fila[22],
                            Producto = fila[23],
                            Moneda = fila[24],
                            TipodePago = fila[29],
                            Bancopagador = fila[25],
                            NdecuentabancariaNTarjeta = fila[26],
                            CuentaNBO = fila[40],
                            CCI = fila[27],
                            Tipodecuentabancaria = fila[28],
                            Estadomonitoreo = fila[37],
                            CodigodeRecargas = fila[31],
                            AcceptorID = fila[32],
                            TerminalID = fila[33],
                            Codigocomerciopatrocinado = fila[11],
                            TipodeRegistro = fila[35],
                            MedioPublicidad = fila[38],
                            AlianzaComercial = fila[39],
                            CodigounicoMC = fila[42],
                            Tipodedocumentodeabono = fila[43],
                            Motivoderechazo = "",
                            Fechaderechazo = "",
                            Empresa = "",
                            Referido = "",
                        };


                        listaComercios.Add(comerENT);
                        counter++;
                    }

                }

                counterF++;               
            };
            

            //enviamos a grabar al aBD en sql server
            SQL_InputGeoComercio(listaENT, listaComercios);

            Console.WriteLine("");
            Console.WriteLine("fecha/hora Termino SQL: {0}", DateTime.UtcNow);
        
        }
        
        public void SQL_InputGeoComercio(List<In_gp_trx_txtBE> listaTrx, List<In_gp_comercios_txtBE> listaCom) {

            //declaramos y abrimos la conexion la BDPROD

            string cadenaCNX = "server=" + serverBD + ";initial catalog=" + nombreBD + ";uid=" + usuarioSQL + ";pwd=" + pwdSQL;
            SqlConnection cnxSQL = new SqlConnection(cadenaCNX);            
            cnxSQL.Open();


            //configuramos el comando para ejecutar el SP de Comercios
            Console.WriteLine("comenzamos con los Comercio's");

            SqlCommand cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "sp_i_IN_GP_COMERCIOS_TXT";

            var progress = new ProgressBar();
            int counter = 0;
            foreach (var item in listaCom)
              //  Parallel.ForEach(listaCom, (item) =>
                {
                    cmdSQL.Parameters.Clear();
                    cmdSQL.Parameters.AddWithValue("@pCodigopadre", item.Codigopadre);
                    cmdSQL.Parameters.AddWithValue("@pEstado", item.Estado);
                    cmdSQL.Parameters.AddWithValue("@pRuccomerciopatrocinado", item.Ruccomerciopatrocinado);
                    cmdSQL.Parameters.AddWithValue("@pIDgeopagos", item.IDgeopagos);
                    cmdSQL.Parameters.AddWithValue("@pRazonSocial", item.RazonSocial);
                    cmdSQL.Parameters.AddWithValue("@pCodigo", item.Codigo);
                    cmdSQL.Parameters.AddWithValue("@pNombrecomercio", item.Nombrecomercio);
                    cmdSQL.Parameters.AddWithValue("@pRepresentantelegal", item.Representantelegal);
                    cmdSQL.Parameters.AddWithValue("@pNúmerodeDocumento", item.NumerodeDocumento);
                    cmdSQL.Parameters.AddWithValue("@pEmailComercial", item.EmailComercial);
                    cmdSQL.Parameters.AddWithValue("@pUsuarioGrupo", item.UsuarioGrupo);
                    cmdSQL.Parameters.AddWithValue("@pCodigoNBO", item.CodigoNBO);
                    cmdSQL.Parameters.AddWithValue("@pDireccionComercial", item.DireccionComercial);
                    cmdSQL.Parameters.AddWithValue("@pDistrito", item.Distrito);
                    cmdSQL.Parameters.AddWithValue("@pProvincia", item.Provincia);
                    cmdSQL.Parameters.AddWithValue("@pDeparmaneto", item.Deparmaneto);
                    cmdSQL.Parameters.AddWithValue("@pTeléfono1", item.Telefono1);
                    cmdSQL.Parameters.AddWithValue("@pUsuarioNombre", item.UsuarioNombre);
                    cmdSQL.Parameters.AddWithValue("@pUsuarioApellido", item.UsuarioApellido);
                    cmdSQL.Parameters.AddWithValue("@pUsuarioCorreo", item.UsuarioCorreo);
                    cmdSQL.Parameters.AddWithValue("@pNrodocumentovendedor", item.Nrodocumentovendedor);
                    cmdSQL.Parameters.AddWithValue("@pTeléfono2", item.Telefono2);
                    cmdSQL.Parameters.AddWithValue("@pCantidaddemPOSComercio", item.CantidaddemPOSComercio);
                    cmdSQL.Parameters.AddWithValue("@pSerialesmpos", item.Serialesmpos);
                    cmdSQL.Parameters.AddWithValue("@pMCC", item.MCC);
                    cmdSQL.Parameters.AddWithValue("@pDescripcionMCC", item.DescripcionMCC);
                    cmdSQL.Parameters.AddWithValue("@pTipoComision", item.TipoComision);
                    cmdSQL.Parameters.AddWithValue("@pComisionCrédito", item.ComisionCredito);
                    cmdSQL.Parameters.AddWithValue("@pComisionDébito", item.ComisionDebito);
                    cmdSQL.Parameters.AddWithValue("@pFechadealtacomerciopatrociando", item.Fechadealtacomerciopatrociando);
                    cmdSQL.Parameters.AddWithValue("@pFechadebajacomerciopatrocinado", item.Fechadebajacomerciopatrocinado);
                    cmdSQL.Parameters.AddWithValue("@pProducto", item.Producto);
                    cmdSQL.Parameters.AddWithValue("@pMoneda", item.Moneda);
                    cmdSQL.Parameters.AddWithValue("@pTipodePago", item.TipodePago);
                    cmdSQL.Parameters.AddWithValue("@pBancopagador", item.Bancopagador);
                    cmdSQL.Parameters.AddWithValue("@pNdecuentabancariaNTarjeta", item.NdecuentabancariaNTarjeta);
                    cmdSQL.Parameters.AddWithValue("@pCuentaNBO", item.CuentaNBO);
                    cmdSQL.Parameters.AddWithValue("@pCCI", item.CCI);
                    cmdSQL.Parameters.AddWithValue("@pTipodecuentabancaria", item.Tipodecuentabancaria);
                    cmdSQL.Parameters.AddWithValue("@pEstadomonitoreo", item.Estadomonitoreo);
                    cmdSQL.Parameters.AddWithValue("@pCodigodeRecargas", item.CodigodeRecargas);
                    cmdSQL.Parameters.AddWithValue("@pAcceptorID", item.AcceptorID);
                    cmdSQL.Parameters.AddWithValue("@pTerminalID", item.TerminalID);
                    cmdSQL.Parameters.AddWithValue("@pCodigocomerciopatrocinado", item.Codigocomerciopatrocinado);
                    cmdSQL.Parameters.AddWithValue("@pTipodeRegistro", item.TipodeRegistro);
                    cmdSQL.Parameters.AddWithValue("@pMedioPublicidad", item.MedioPublicidad);
                    cmdSQL.Parameters.AddWithValue("@pAlianzaComercial", item.AlianzaComercial);
                    cmdSQL.Parameters.AddWithValue("@pCodigounicoMC", item.CodigounicoMC);
                    cmdSQL.Parameters.AddWithValue("@pTipodedocumentodeabono", item.Tipodedocumentodeabono);
                    cmdSQL.Parameters.AddWithValue("@pMotivoderechazo", item.Motivoderechazo);
                    cmdSQL.Parameters.AddWithValue("@pFechaderechazo", item.Fechaderechazo);
                    cmdSQL.Parameters.AddWithValue("@pEmpresa", item.Empresa);
                    cmdSQL.Parameters.AddWithValue("@pReferido", item.Referido);
                    cmdSQL.Parameters.AddWithValue("@pIdProceso", item.IdProceso);

                    cmdSQL.ExecuteNonQuery();

                    counter++;
                    progress.Report((double)counter / listaCom.Count);
                    //Thread.Sleep(20);
                };

            Console.WriteLine("");
            Console.WriteLine("comenzamos con las TRX's");

            //configuramos el comando para ejecutar el SP de Trx

            cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "sp_i_in_gp_trx_txt";

            //iteramos la lista Trx y ejecutamos cada fila
            counter = 0;
            foreach (var item in listaTrx)
            //Parallel.ForEach(listaTrx, (item) =>
            {
                cmdSQL.Parameters.Clear();
                cmdSQL.Parameters.AddWithValue("@pTipodeTransacción", item.TipodeTransaccion);
                cmdSQL.Parameters.AddWithValue("@pFechaTransacción", item.FechaTransaccion);
                cmdSQL.Parameters.AddWithValue("@pCódigoMCC", item.CodigoMCC);
                cmdSQL.Parameters.AddWithValue("@pActividad", item.Actividad);
                cmdSQL.Parameters.AddWithValue("@pTerminalID", item.TerminalID);
                cmdSQL.Parameters.AddWithValue("@pIDgeopagos", item.IDgeopagos);
                cmdSQL.Parameters.AddWithValue("@pCódigodeComercio", item.CodigodeComercio);
                cmdSQL.Parameters.AddWithValue("@pCódigoUsuario", item.CodigoUsuario);
                cmdSQL.Parameters.AddWithValue("@pNombreUsuario", item.NombreUsuario);
                cmdSQL.Parameters.AddWithValue("@pCorreoUsuario", item.CorreoUsuario);
                cmdSQL.Parameters.AddWithValue("@pNombredeComercio", item.NombredeComercio);
                cmdSQL.Parameters.AddWithValue("@pCódigodeLocal", item.CodigodeLocal);
                cmdSQL.Parameters.AddWithValue("@pNombredeLocal", item.NombredeLocal);
                cmdSQL.Parameters.AddWithValue("@pCuentadeabono", item.Cuentadeabono);
                cmdSQL.Parameters.AddWithValue("@pTipodedocumentodeabono", item.Tipodedocumentodeabono);
                cmdSQL.Parameters.AddWithValue("@pDocumentodeabono", item.Documentodeabono);
                cmdSQL.Parameters.AddWithValue("@pNombredeabono", item.Nombredeabono);
                cmdSQL.Parameters.AddWithValue("@pTipodePago", item.TipodePago);
                cmdSQL.Parameters.AddWithValue("@pNombredeBanco", item.NombredeBanco);
                cmdSQL.Parameters.AddWithValue("@pNdecuentabancariaNTarjeta", item.NdecuentabancariaNTarjeta);
                cmdSQL.Parameters.AddWithValue("@pCuentaNBO", item.CuentaNBO);
                cmdSQL.Parameters.AddWithValue("@pNúmerodecuentainterbancaria", item.Numerodecuentainterbancaria);
                cmdSQL.Parameters.AddWithValue("@pTipodecuentabancaria", item.Tipodecuentabancaria);
                cmdSQL.Parameters.AddWithValue("@pTipodetarjeta", item.Tipodetarjeta);
                cmdSQL.Parameters.AddWithValue("@pRepresentanteLegal", item.RepresentanteLegal);
                cmdSQL.Parameters.AddWithValue("@pEstado", item.Estado);
                cmdSQL.Parameters.AddWithValue("@pDescripciónestado", item.Descripcionestado);
                cmdSQL.Parameters.AddWithValue("@pRespuestaVisa", item.RespuestaVisa);
                cmdSQL.Parameters.AddWithValue("@pCorreoComprador", item.CorreoComprador);
                cmdSQL.Parameters.AddWithValue("@pNombreComprador", item.NombreComprador);
                cmdSQL.Parameters.AddWithValue("@pMarcadetarjeta", item.Marcadetarjeta);
                cmdSQL.Parameters.AddWithValue("@pNúmerodetarjeta", item.Numerodetarjeta);
                cmdSQL.Parameters.AddWithValue("@pGeolocalización", item.Geolocalizacion);
                cmdSQL.Parameters.AddWithValue("@pMoneda", item.Moneda);
                cmdSQL.Parameters.AddWithValue("@pTransactionID", item.TransactionID);
                cmdSQL.Parameters.AddWithValue("@pCódigodeoperación", item.Codigodeoperacion);
                cmdSQL.Parameters.AddWithValue("@pNderefetencia", item.Nderefetencia);
                cmdSQL.Parameters.AddWithValue("@pNdeautorización", item.Ndeautorizacion);
                cmdSQL.Parameters.AddWithValue("@pNdesecuencia", item.Ndesecuencia);
                cmdSQL.Parameters.AddWithValue("@pMontotransacción", item.Montotransaccion);
                cmdSQL.Parameters.AddWithValue("@pTipodecampaña", item.Tipodecampana);
                cmdSQL.Parameters.AddWithValue("@pTipoComisión", item.TipoComision);
                cmdSQL.Parameters.AddWithValue("@pPorcentajeComisión", item.PorcentajeComision);
                cmdSQL.Parameters.AddWithValue("@pImportedecomisiónVendeMás", item.ImportedecomisionVendeMas);
                cmdSQL.Parameters.AddWithValue("@pImporteIGVdecomisiónVendeMás", item.ImporteIGVdecomisionVendeMas);
                cmdSQL.Parameters.AddWithValue("@pImportenetoadepositar", item.Importenetoadepositar);
                cmdSQL.Parameters.AddWithValue("@pDispositivo", item.Dispositivo);
                cmdSQL.Parameters.AddWithValue("@pPorcentajedelacomisiónVisanet", item.PorcentajedelacomisionVisanet);
                cmdSQL.Parameters.AddWithValue("@pMontodelacomisiónVisanet", item.MontodelacomisionVisanet);
                cmdSQL.Parameters.AddWithValue("@pImpuestodelacomisiónVisanet", item.ImpuestodelacomisionVisanet);
                cmdSQL.Parameters.AddWithValue("@pComisiónaldescargar", item.Comisionaldescargar);
                cmdSQL.Parameters.AddWithValue("@pCódigoNBO", item.CodigoNBO);
                cmdSQL.Parameters.AddWithValue("@pAlianzaComercial", item.AlianzaComercial);
                cmdSQL.Parameters.AddWithValue("@pEstadomonitoreo", item.Estadomonitoreo);
                cmdSQL.Parameters.AddWithValue("@pCódigoRecargasServicios", item.CodigoRecargasServicios);
                cmdSQL.Parameters.AddWithValue("@pCódigoMC", item.CodigoMC);
                cmdSQL.Parameters.AddWithValue("@pFuente", item.Fuente);
                cmdSQL.Parameters.AddWithValue("@pIdProceso", item.IdProceso);

                cmdSQL.ExecuteNonQuery();

                counter++;
                progress.Report((double)counter / listaTrx.Count);
                //Thread.Sleep(20);
            };

            Console.WriteLine("");
            progress = null;

            //cerramos y liberamos la conexion
            cnxSQL.Close();
            cnxSQL.Dispose();

        }
                       
    }
}
