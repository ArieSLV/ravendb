﻿using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class LicenseRequiredFactAttribute : FactAttribute
    {
        private static readonly bool ShouldSkip;

        internal static string SkipMessage = "Requires License to be set via 'RAVEN_LICENSE' environment variable.";

        static LicenseRequiredFactAttribute()
        {
            ShouldSkip = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE"));
        }

        public override string Skip
        {
            get
            {
                if (ShouldSkip)
                    return SkipMessage;

                return null;
            }
        }
    }
}
