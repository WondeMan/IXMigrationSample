using IXMIgrationSample.GetIXData;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text; 
using System.Threading.Tasks;
using System.Timers;

namespace IXMIgrationSample
{
    public partial class IXMigration : ServiceBase
    {
        private Timer timer;
        public IXMigration()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            timer = new Timer();
            timer.Interval = 60000 * 40;
            timer.Elapsed += new ElapsedEventHandler(ReadIX);
        }

        public void ReadIX(object sender, ElapsedEventArgs e)
        {
            new GetTicketDetail().ComputeAdvancedPaxCount();
        }

        public void debug()
        {

        }
        protected override void OnStop()
        {
            
        }
    }
}
