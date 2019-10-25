using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppLeerInputs.Models
{
    public class In_pmcBE
    {
        public string Codigo { get; set; }
        public string Producto { get; set; }
        public string Tipo_Mov { get; set; }
        public string Fecha_Proceso { get; set; }
        public string Fecha_Lote { get; set; }
        public string Lote_Manual { get; set; }
        public string Lote_Pos { get; set; }
        public string Terminal { get; set; }
        public string Voucher { get; set; }
        public string Autorizacion { get; set; }
        public string Cuotas { get; set; }
        public string Tarjeta { get; set; }
        public string Origen { get; set; }
        public string Transaccion { get; set; }
        public string Fecha_Consumo { get; set; }
        public double Importe { get; set; }
        public string Status { get; set; }
        public double Comision { get; set; }
        public double Comision_Afecta { get; set; }
        public double IGV { get; set; }
        public double Neto_Parcial { get; set; }
        public double Neto_Total { get; set; }
        public string Fecha_Abono { get; set; }
        public string Fecha_Abono_8Dig { get; set; }
        public string Observaciones { get; set; }
        public double ExtraComision { get; set; }
        public double Comis_Standar { get; set; }
        public double Comis { get; set; }
        public string Nro_ID { get; set; }
        public string Tpo_Comprob { get; set; }
        public string Nro_Comprob { get; set; }
        public int Idproceso { get; set; }      
    }
}
