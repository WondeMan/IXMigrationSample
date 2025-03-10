﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace IXMIgrationSample
{
    internal static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
#if (DEBUG)
            { 
            
            IXMigration iXMigration= new IXMigration();
                iXMigration.debug();
                iXMigration.ReadIX(null, null);

            }

#else
                ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new IXMigration()
            };
            ServiceBase.Run(ServicesToRun);
#endif
        }
    }
}
