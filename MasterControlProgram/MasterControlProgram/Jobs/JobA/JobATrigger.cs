using System;

namespace MasterControlProgram.Jobs.JobA
{
    public enum JobATrigger
    {
        Initialized,
        Worker1Start,
        Worker1Complete,
        Worker1Error,
        Worker2Start,
        Worker2Complete,
        Worker2Error,
        JobFinished
    }
}
