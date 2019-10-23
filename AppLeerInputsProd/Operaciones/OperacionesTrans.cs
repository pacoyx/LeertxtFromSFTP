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
                       
            //StreamReader file = new StreamReader(rutaArchivo);
            //while ((line = file.ReadLine()) != null)
            //{
            //    var fila = line.Split('\t');
            //    In_gp_trx_txtBE ent = new In_gp_trx_txtBE()
            //    {
            //        IdProceso = IdProceso,
            //        TipodeTransaccion = fila[0],
            //        FechaTransaccion = fila[1],
            //        CodigoMCC = fila[2],
            //        Actividad = fila[3],
            //        TerminalID = fila[4],
            //        IDgeopagos = fila[5],
            //        CodigodeComercio = fila[6],
            //        CodigoUsuario = fila[7],
            //        NombreUsuario = fila[8],
            //        CorreoUsuario = fila[9],
            //        NombredeComercio = fila[10],
            //        CodigodeLocal = fila[11],
            //        NombredeLocal = fila[12],
            //        Cuentadeabono = fila[13],
            //        Tipodedocumentodeabono = fila[14],
            //        Documentodeabono = fila[15],
            //        Nombredeabono = fila[16],
            //        TipodePago = fila[17],
            //        NombredeBanco = fila[18],
            //        NdecuentabancariaNTarjeta = fila[19],
            //        CuentaNBO = fila[20],
            //        Numerodecuentainterbancaria = fila[21],
            //        Tipodecuentabancaria = fila[22],
            //        Tipodetarjeta = fila[23],
            //        RepresentanteLegal = fila[24],
            //        Estado = fila[25],
            //        Descripcionestado = fila[26],
            //        RespuestaVisa = fila[27],
            //        CorreoComprador = fila[28],
            //        NombreComprador = fila[29],
            //        Marcadetarjeta = fila[30],
            //        Numerodetarjeta = fila[31],
            //        Geolocalizacion = fila[32],
            //        Moneda = fila[33],
            //        TransactionID = fila[34],
            //        Codigodeoperacion = fila[35],
            //        Nderefetencia = fila[36],
            //        Ndeautorizacion = fila[37],
            //        Ndesecuencia = fila[38],
            //        Montotransaccion = fila[39],
            //        Tipodecampana = fila[40],
            //        TipoComision = fila[41],
            //        PorcentajeComision = fila[42],
            //        ImportedecomisionVendeMas = fila[43],
            //        ImporteIGVdecomisionVendeMas = fila[44],
            //        Importenetoadepositar = fila[45],
            //        Dispositivo = fila[46],
            //        PorcentajedelacomisionVisanet = fila[47],
            //        MontodelacomisionVisanet = fila[48],
            //        ImpuestodelacomisionVisanet = fila[49],
            //        Comisionaldescargar = fila[50],
            //        CodigoNBO = fila[51],
            //        AlianzaComercial = fila[52],
            //        Estadomonitoreo = fila[53],
            //        CodigoRecargasServicios = fila[54],
            //        CodigoMC = fila[55],
            //        Fuente = fila[56],
            //    };
            //    listaENT.Add(ent);
            //}


            
            //leemos el archivo txt de comercios(2xmin) GeoPagos
            string[] files = Directory.GetFiles(this.carpetaLocal + carpeta);
            int counter = 0;
            foreach (var item in files)
            {
                if (!item.Contains(nomArchivoTRX))
                {
                    StreamReader fileC = new StreamReader(item);
                    while ((line = fileC.ReadLine()) != null)
                    {
                        var fila = line.Split('\t');
                        In_gp_comercios_txtBE comerENT = new In_gp_comercios_txtBE() {
                            pIdProceso = IdProceso,
                            @pCodigopadre = fila[0],
                            @pEstado = fila[1],
                            @pRuccomerciopatrocinado = fila[2],
                            @pIDgeopagos = fila[3],
                            @pRazonSocial = fila[4],
                            @pCodigo = fila[5],
                            @pNombrecomercio = fila[6],
                            @pRepresentantelegal = fila[7],
                            @pNúmerodeDocumento = fila[8],
                            @pEmailComercial = fila[9],
                            @pUsuarioGrupo = "",
                            @pCodigoNBO = fila[11],
                            @pDireccionComercial = fila[12],
                            @pDistrito = fila[13],
                            @pProvincia = fila[14],
                            @pDeparmaneto = fila[15],

                            @pTeléfono1 = fila[10],
                            @pUsuarioNombre ="",    
                            @pUsuarioApellido = "",
                            @pUsuarioCorreo = ""                                                                                                ,
                            @pNrodocumentovendedor = fila[8],
                            @pTeléfono2 = fila[10],
                            @pCantidaddemPOSComercio = "",
                            @pSerialesmpos = "",

                            @pMCC = fila[16],
                            @pDescripcionMCC = fila[17],
                            @pTipoComision = fila[18],
                            @pComisionCrédito = fila[19],
                            @pComisionDébito = fila[20],
                            @pFechadealtacomerciopatrociando = fila[21],
                            @pFechadebajacomerciopatrocinado = fila[22],
                            @pProducto = fila[23],
                            @pMoneda = fila[24],
                            @pTipodePago = fila[29],
                            @pBancopagador = fila[25],
                            @pNdecuentabancariaNTarjeta = fila[26],
                            @pCuentaNBO = fila[40],
                            @pCCI = fila[27],
                            @pTipodecuentabancaria = fila[28],
                            @pEstadomonitoreo = fila[37],
                            @pCodigodeRecargas = fila[31],
                            @pAcceptorID = fila[32],
                            @pTerminalID = fila[33],
                            @pCodigocomerciopatrocinado = fila[11],
                            @pTipodeRegistro = fila[35],
                            @pMedioPublicidad = fila[38],
                            @pAlianzaComercial = fila[39],
                            @pCodigounicoMC = fila[42],
                            @pTipodedocumentodeabono = fila[43],
                            @pMotivoderechazo = "",
                            @pFechaderechazo = "",
                            @pEmpresa = "",
                            @pReferido = ""
                        };


                        listaComercios.Add(comerENT);
                        counter++;
                    }

                }
            }



            SQL_InputGeoComercio(listaENT, listaComercios);

            Console.WriteLine("fecha/hora Termino SQL: {0}", DateTime.UtcNow);
        }
        
        public void SQL_InputGeoComercio(List<In_gp_trx_txtBE> listaTrx, List<In_gp_comercios_txtBE> listaCom) {

            //declaramos y abrimos la conexion la BDPROD

            string cadenaCNX = "server=" + serverBD + ";initial catalog=" + nombreBD + ";uid=" + usuarioSQL + ";pwd=" + pwdSQL;
            SqlConnection cnxSQL = new SqlConnection(cadenaCNX);            
            cnxSQL.Open();


            //configuramos el comando para ejecutar el SP de Comercios

            SqlCommand cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "sp_i_IN_GP_COMERCIOS_TXT";

            foreach (var item in listaCom)
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
            }



            //configuramos el comando para ejecutar el SP de Trx

            cmdSQL = new SqlCommand();
            cmdSQL.Connection = cnxSQL;
            cmdSQL.CommandType = CommandType.StoredProcedure;
            cmdSQL.CommandText = "sp_i_in_gp_trx_txt";
            
            //iteramos la lista Trx y ejecutamos cada fila
            foreach (var item in listaTrx)
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
            }
                       
            //cerramos y liberamos la conexion

            cnxSQL.Close();
            cnxSQL.Dispose();

        }
                       
    }
}
