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
using NLog;
using MongoDB.Driver;

namespace AppLeerInputs.Operaciones
{
    public class OperacionesTrans
    {
        string carpetaLocal = "";
        string carpetaLocal_PMC = "";
        string serverBD = "";
        string nombreBD = "";
        string usuarioSQL = "";
        string pwdSQL = "";


        Logger loggerx = LogManager.GetCurrentClassLogger();


        public OperacionesTrans(int idproceso) {
            LeerConfiguracion_Geo(idproceso);
        }

        public OperacionesTrans()
        {
            LeerConfiguracion_Geo(0);
        }

        private void LeerConfiguracion_Geo(int idproceso)
        {
            this.carpetaLocal = ConfigurationManager.AppSettings["carpetaLocal_geo"].ToString();
            this.carpetaLocal_PMC = ConfigurationManager.AppSettings["carpetaLocal_pmc"].ToString();

            this.serverBD = ConfigurationManager.AppSettings["serverBD_xzedi"].ToString();
            this.nombreBD = ConfigurationManager.AppSettings["nombreBD_xzedi"].ToString();
            this.usuarioSQL = ConfigurationManager.AppSettings["usuarioSQL_xzedi"].ToString();
            this.pwdSQL = ConfigurationManager.AppSettings["pwdSQL_xzedi"].ToString();
                        
            loggerx.Info(idproceso+"|Lectura de parametros de conexion a BD completo");

            if (this.carpetaLocal == string.Empty ||
               this.serverBD == string.Empty ||
               this.nombreBD == string.Empty ||
               this.usuarioSQL == string.Empty ||
               this.pwdSQL == string.Empty                
               )
            {
                loggerx.Warn(idproceso + "|Carga de parametros para conexion a BD incompleto.");
            }

        }
                     
        public void Grabar_Input_NBO(int idproceso, string carpeta)
        {

        }

        public void Grabar_InputPMC(int idproceso, string carpeta)
        {
            string rutaArchivo = this.carpetaLocal_PMC + carpeta  + @"\mc_009018443.csv";
            string line;
            List<In_pmcBE> listaENT = new List<In_pmcBE>();

            if (!File.Exists(rutaArchivo))
            {
                string mensaje = idproceso + "|No existe el archivo csv PMC en la ruta: " + rutaArchivo;
                Console.WriteLine(mensaje);
                loggerx.Warn(mensaje);
            }
            else
            {
                int cont = 0;
                StreamReader file = new StreamReader(rutaArchivo);
                while ((line = file.ReadLine()) != null)
                {
                    if (cont > 0)
                    {
                        var fila = line.Split(';');
                        In_pmcBE ent = new In_pmcBE()
                        {
                            Idproceso = idproceso,
                            Codigo = fila[0],
                            Producto = fila[1],
                            Tipo_Mov = fila[2],
                            Fecha_Proceso = fila[3],
                            Fecha_Lote = fila[4],
                            Lote_Manual = fila[5],
                            Lote_Pos = fila[6],
                            Terminal = fila[7],
                            Voucher = fila[8],
                            Autorizacion = fila[9],
                            Cuotas = fila[10],
                            Tarjeta = fila[11],
                            Origen = fila[12],
                            Transaccion = fila[13],
                            Fecha_Consumo = fila[14],
                            Importe = double.Parse(fila[15]),
                            Status = fila[16],
                            Comision = double.Parse(fila[17]),
                            Comision_Afecta = double.Parse(fila[18]),
                            IGV = double.Parse(fila[19]),
                            Neto_Parcial = double.Parse(fila[20]),
                            Neto_Total = double.Parse(fila[21]),
                            Fecha_Abono = fila[22],
                            Fecha_Abono_8Dig = fila[23],
                            Observaciones = fila[24],
                            ExtraComision = double.Parse(fila[25]),
                            Comis_Standar = double.Parse(fila[26]),
                            Comis = double.Parse(fila[27]),
                            Nro_ID = fila[28],
                            Tpo_Comprob = fila[29],
                            Nro_Comprob = fila[30]
                        };
                        listaENT.Add(ent);
                    }
                    cont++;
                }
            }            

            SQL_Input_PMC(idproceso, listaENT);
        }

        public void Grabar_InputGeoPagos(int  idproceso,string nomArchivoTRX, string carpeta)
        {
            string line;
            List<In_gp_trx_txtBE> lista_Trxs_Geo = new List<In_gp_trx_txtBE>();
            List<In_gp_comercios_txtBE> lista_Comercios_Geo = new List<In_gp_comercios_txtBE>();
            string rutaArchivo = this.carpetaLocal + carpeta + @"\" + nomArchivoTRX + ".txt";

            if (!File.Exists(rutaArchivo))
            {
                string mensaje = idproceso + "|No existe el archivo txt geopagos TRX en la ruta: " + rutaArchivo;
                Console.WriteLine(mensaje);
                loggerx.Warn(mensaje);
            }
            else
            {
                Console.WriteLine("fecha/hora inicio SQL: {0}", DateTime.UtcNow);

                //leemos el archivo txt de transacciones GeoPagos
                StreamReader file = new StreamReader(rutaArchivo);
                while ((line = file.ReadLine()) != null)
                {
                    var fila = line.Split('\t');
                    In_gp_trx_txtBE ent = new In_gp_trx_txtBE()
                    {
                        IdProceso = idproceso,
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
                        Fuente = fila[56]
                    };
                    lista_Trxs_Geo.Add(ent);
                };
            }
            

            //llemos el archivo txt de comercios(2xmin) GeoPagos
            string[] files = Directory.GetFiles(this.carpetaLocal + carpeta);
            int counter = 0;
            int counterF = 0;
         
            foreach (var item in files)
            {                           
                if (!item.Contains(nomArchivoTRX))
                {
                    //comercios_20191016_230805.txt
                    string fechacadena = item.ToString().Substring(item.Length - 19, 15);

                    string yy = fechacadena.Substring(0, 4);
                    string MM = fechacadena.Substring(4, 2);
                    string dd = fechacadena.Substring(6, 2);
                    string hh = fechacadena.Substring(9, 2);
                    string mi = fechacadena.Substring(11, 2);
                    string ss = fechacadena.Substring(13, 2);

                    DateTime fechaCreacionReg = new DateTime(int.Parse(yy), int.Parse(MM), int.Parse(dd), int.Parse(hh), int.Parse(mi), int.Parse(ss));
                                                 
                    StreamReader fileC = new StreamReader(item);
                    while ((line = fileC.ReadLine()) != null)
                    {
                        var fila = line.Split('\t');
                        In_gp_comercios_txtBE comerENT = new In_gp_comercios_txtBE() {

                            IdProceso = idproceso,
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
                            Fechacreacionreg = fechaCreacionReg,
                        };

                        lista_Comercios_Geo.Add(comerENT);
                        counter++;
                    }

                }

                counterF++;               
            };
                       
            if(lista_Comercios_Geo.Count == 0)
            {
                string mensaje = idproceso + "|No existen archivos txt geopagos comercios en la ruta: " + this.carpetaLocal + carpeta;
                Console.WriteLine(mensaje);
                loggerx.Warn(mensaje);
            }

            loggerx.Info(idproceso+"|Generacion de lista de entidades de trx y comercios completo");

            //  enviamos a grabar a la BD en sql server ____________________________________________________________________________
            SQL_InputGeoComercio(idproceso, lista_Trxs_Geo, lista_Comercios_Geo);
            
            //  enviamos a garbar a la BD MongoDB
            NOSQL_InputGeoComercio_y_Trxs(idproceso, lista_Trxs_Geo, lista_Comercios_Geo);
           

            Console.WriteLine("");
            Console.WriteLine("fecha/hora Termino SQL: {0}", DateTime.UtcNow);
        
        }

        public void SQL_Input_PMC(int idproceso, List<In_pmcBE> listapmc)
        {
            string cadenaCNX = "server=" + serverBD + ";initial catalog=" + nombreBD + ";uid=" + usuarioSQL + ";pwd=" + pwdSQL;
            SqlConnection cnxSQL = new SqlConnection(cadenaCNX);
            cnxSQL.Open();

            SqlCommand cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "sp_i_in_pmc";

            var progress = new ProgressBar();
            int counter = 0;
            try
            {
                foreach (var item in listapmc)                
                {
                    cmdSQL.Parameters.Clear();
                    cmdSQL.Parameters.AddWithValue("@pCodigo", item.Codigo);
                    cmdSQL.Parameters.AddWithValue("@pProducto", item.Producto);
                    cmdSQL.Parameters.AddWithValue("@pTipo_Mov", item.Tipo_Mov);
                    cmdSQL.Parameters.AddWithValue("@pFecha_Proceso", item.Fecha_Proceso);
                    cmdSQL.Parameters.AddWithValue("@pFecha_Lote", item.Fecha_Lote);
                    cmdSQL.Parameters.AddWithValue("@pLote_Manual", item.Lote_Manual);
                    cmdSQL.Parameters.AddWithValue("@pLote_Pos", item.Lote_Pos);
                    cmdSQL.Parameters.AddWithValue("@pTerminal", item.Terminal);
                    cmdSQL.Parameters.AddWithValue("@pVoucher", item.Voucher);
                    cmdSQL.Parameters.AddWithValue("@pAutorizacion", item.Autorizacion);
                    cmdSQL.Parameters.AddWithValue("@pCuotas", item.Cuotas);
                    cmdSQL.Parameters.AddWithValue("@pTarjeta", item.Tarjeta);
                    cmdSQL.Parameters.AddWithValue("@pOrigen", item.Origen);
                    cmdSQL.Parameters.AddWithValue("@pTransaccion", item.Transaccion);
                    cmdSQL.Parameters.AddWithValue("@pFecha_Consumo", item.Fecha_Consumo);
                    cmdSQL.Parameters.AddWithValue("@pImporte", item.Importe);
                    cmdSQL.Parameters.AddWithValue("@pStatus", item.Status);
                    cmdSQL.Parameters.AddWithValue("@pComision", item.Comision);
                    cmdSQL.Parameters.AddWithValue("@pComision_Afecta", item.Comision_Afecta);
                    cmdSQL.Parameters.AddWithValue("@pIGV", item.IGV);
                    cmdSQL.Parameters.AddWithValue("@pNeto_Parcial", item.Neto_Parcial);
                    cmdSQL.Parameters.AddWithValue("@pNeto_Total", item.Neto_Total);
                    cmdSQL.Parameters.AddWithValue("@pFecha_Abono", item.Fecha_Abono);
                    cmdSQL.Parameters.AddWithValue("@pFecha_Abono_8Dig", item.Fecha_Abono_8Dig);
                    cmdSQL.Parameters.AddWithValue("@pObservaciones", item.Observaciones);
                    cmdSQL.Parameters.AddWithValue("@pExtraComision", item.ExtraComision);
                    cmdSQL.Parameters.AddWithValue("@pComis_Standar", item.Comis_Standar);
                    cmdSQL.Parameters.AddWithValue("@pComis", item.Comis);
                    cmdSQL.Parameters.AddWithValue("@pNro_ID", item.Nro_ID);
                    cmdSQL.Parameters.AddWithValue("@pTpo_Comprob", item.Tpo_Comprob);
                    cmdSQL.Parameters.AddWithValue("@pNro_Comprob", item.Nro_Comprob);
                    cmdSQL.Parameters.AddWithValue("@pIdproceso", item.Idproceso);

                    cmdSQL.ExecuteNonQuery();

                    counter++;
                    progress.Report((double)counter / listapmc.Count);
               
                };
            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso + "|Ocurrio un error en el proceso de insertar datos de PMC en la BD");
            }
            Console.WriteLine("");            
            loggerx.Info(idproceso + "|insercion de registros PMC en BD completo");


            //actualizamos el estado _________________________________________________________________________________________________________
            try
            {
                cmdSQL = new SqlCommand();
                cmdSQL.Connection = cnxSQL;
                cmdSQL.CommandType = CommandType.StoredProcedure;
                cmdSQL.CommandText = "SP_U_ESTADO_PROCESO";
                cmdSQL.Parameters.Clear();
                cmdSQL.Parameters.AddWithValue("@IDPROCESOIDENTITY", idproceso);
                cmdSQL.Parameters.AddWithValue("@IDSUBPROCESO", 1);
                cmdSQL.Parameters.AddWithValue("@INPUTFILE", "PMC");
                cmdSQL.ExecuteNonQuery();

                loggerx.Info(idproceso + "|Se actualizo correctamente el estado del proceso carga de Transacciones Geopagos");

            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso + "|Ocurrio al intentar actualizar proceso de Transacciones Geopagos");
            }

        }

        public void SQL_InputGeoComercio(int idproceso,List<In_gp_trx_txtBE> listaTrx, List<In_gp_comercios_txtBE> listaCom) {

            //declaramos y abrimos la conexion la BDPROD_________________________________________________________________________

            string cadenaCNX = "server=" + serverBD + ";initial catalog=" + nombreBD + ";uid=" + usuarioSQL + ";pwd=" + pwdSQL;
            SqlConnection cnxSQL = new SqlConnection(cadenaCNX);            
            cnxSQL.Open();


            //Carga de Comercios GeoPagos ______________________________________________________________________________________
            Console.WriteLine("comenzamos con los Comercio's");

            SqlCommand cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "sp_i_IN_GP_COMERCIOS_TXT";

            var progress = new ProgressBar();
            int counter = 0;
            try {
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
                    cmdSQL.Parameters.AddWithValue("@pfechacreacionreg", item.Fechacreacionreg);

                    cmdSQL.ExecuteNonQuery();

                    counter++;
                    progress.Report((double)counter / listaCom.Count);
                    //Thread.Sleep(20);
                };
            }
            catch (Exception ex)
            {
                loggerx.Error(ex,idproceso+"|Ocurrio un error en el proceso de insertar datos de comercios en la BD");
            }
            Console.WriteLine("");
            Console.WriteLine("comenzamos con las TRX's");

            loggerx.Info(idproceso + "|insercion de comercios en BD completo");

            //actualizamos el estado de subproceso comercios______________________________________________________________________________
            try
            {
                cmdSQL = new SqlCommand();
                cmdSQL.Connection = cnxSQL;
                cmdSQL.CommandType = CommandType.StoredProcedure;
                cmdSQL.CommandText = "SP_U_ESTADO_PROCESO";
                cmdSQL.Parameters.Clear();
                cmdSQL.Parameters.AddWithValue("@IDPROCESOIDENTITY", idproceso);
                cmdSQL.Parameters.AddWithValue("@IDSUBPROCESO", 1);
                cmdSQL.Parameters.AddWithValue("@INPUTFILE", "GEOCO");
                cmdSQL.ExecuteNonQuery();

                loggerx.Info(idproceso + "|Se actualizo correctamente el estado del proceso carga de comercios");

            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso + "|Ocurrio al intentar actualizar proceso de comercios");
            }




            // SP de Trx de GeoPagos_______________________________________________________________________________________________________

            cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "sp_i_in_gp_trx_txt";
         
            counter = 0;
            try
            {
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
            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso + "|Ocurrio un error en el proceso de insertar datos de trx's en la BD");
            }
            loggerx.Info(idproceso+"|insercion de transacciones en BD completo");

            //actualizamos el estado _________________________________________________________________________________________________________
            try
            {
                cmdSQL = new SqlCommand();
                cmdSQL.Connection = cnxSQL;
                cmdSQL.CommandType = CommandType.StoredProcedure;
                cmdSQL.CommandText = "SP_U_ESTADO_PROCESO";
                cmdSQL.Parameters.Clear();
                cmdSQL.Parameters.AddWithValue("@IDPROCESOIDENTITY", idproceso);
                cmdSQL.Parameters.AddWithValue("@IDSUBPROCESO", 1);
                cmdSQL.Parameters.AddWithValue("@INPUTFILE", "GEOTX");
                cmdSQL.ExecuteNonQuery();

                loggerx.Info(idproceso + "|Se actualizo correctamente el estado del proceso carga de Transacciones Geopagos");

            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso + "|Ocurrio al intentar actualizar proceso de Transacciones Geopagos");
            }




            // SP que eliminar los registros duplicados de comercios geopagos_______________________________________________________________________________________________________

            try
            {
                cmdSQL = new SqlCommand();
                cmdSQL.Connection = cnxSQL;
                cmdSQL.CommandType = CommandType.StoredProcedure;
                cmdSQL.CommandText = "sp_d_eliminar_duplicado_txtgeo";
                cmdSQL.Parameters.Clear();
                cmdSQL.Parameters.AddWithValue("@vidproceso", idproceso);
                cmdSQL.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                loggerx.Error(ex, idproceso + "|Ocurrio al intentar eliminar duplicados en BD comercios");
            }

            loggerx.Info(idproceso + "|Eliminando duplicados en comercios BD");


            Console.WriteLine("");
            progress = null;

           

          
            //cerramos y liberamos la conexion _____________________________________________________________________
            cnxSQL.Close();
            cnxSQL.Dispose();

        }

        public void NOSQL_InputGeoComercio_y_Trxs(int idproceso, List<In_gp_trx_txtBE> listaTrx, List<In_gp_comercios_txtBE> listaCom) {
            //lee archivos de la carpeta donde esta los comercios de geo (cada 2 minutos)  
            //y graba en Atlas MongoDB

            Console.WriteLine("Conectando a ATLAS......");

            NoSql.MongoHelper.ConnectToMongoService();
            NoSql.MongoHelper.comercios_collection = NoSql.MongoHelper.database.GetCollection<Models.In_gp_comercios_txtBE>("merchants");
            NoSql.MongoHelper.trxs_collection = NoSql.MongoHelper.database.GetCollection<Models.In_gp_trx_txtBE>("trxs");

            Console.WriteLine("Conectados.....");


            var groupedList = from comer in listaCom
                              group comer by comer.CodigoNBO into nboGroup
                              where nboGroup.Count() > 1
                              orderby nboGroup.Key ascending
                              select nboGroup;
            Console.WriteLine("numero de filas: {0}", listaCom.Count());
            foreach (var group in groupedList)
            {
                int ccon = 1;
                Console.WriteLine(string.Format("CodigoNBO: {0}", group.Key));
                foreach (var comercio in group)
                {

                    if (ccon < group.Count())
                    {
                        Console.WriteLine(string.Format("\t Comercio: {0} - {1} Eliminado", comercio.RazonSocial, comercio.Fechacreacionreg));
                        listaCom.Remove(comercio);
                    }
                    else
                    {
                        Console.WriteLine(string.Format("\t Comercio: {0} - {1} queda!!!!", comercio.RazonSocial, comercio.Fechacreacionreg));
                    }

                    ccon++;
                }
            }

            Console.WriteLine("numero de filas despues de la depuracion: {0}", listaCom.Count());


            foreach (var item in listaTrx)
            {
                NoSql.MongoHelper.trxs_collection.InsertOneAsync(item);
            }

            foreach (var item in listaCom)
            {
                NoSql.MongoHelper.comercios_collection.InsertOneAsync(item);
            }

           

            Console.WriteLine("proceso NoSql terminado");

        }

        private static Random random = new Random();
        private object GenerateRamdonId(int v)
        {
            string strarray = "abcdefghijklmnopqrstuvwxyz123456789";
            return new string(Enumerable.Repeat(strarray, v).Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public int SQL_Crea_Obtiene_IdProceso(string usuario) {
            int IdProceso_Rpta = 0;

            string cadenaCNX = "server=" + serverBD + ";initial catalog=" + nombreBD + ";uid=" + usuarioSQL + ";pwd=" + pwdSQL;
            SqlConnection cnxSQL = new SqlConnection(cadenaCNX);
            cnxSQL.Open();

            SqlCommand cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "SP_I_CREA_PROCESO";
            cmdSQL.Parameters.Clear();
            cmdSQL.Parameters.AddWithValue("@USUARIO", usuario);
            try
            {
                IdProceso_Rpta = (int)cmdSQL.ExecuteScalar();
                loggerx.Info(IdProceso_Rpta+ "|Se genero y obtuvo proceso identity ok");
            }
            catch (Exception ex)
            {
                loggerx.Fatal(ex, "0|Ocurrio un error al obtener proceso identity");
            }

            cnxSQL.Close();
            cnxSQL.Dispose();

            return IdProceso_Rpta;
        }

    }
}
