using System;

namespace MasterControlProgram.Jobs.JobA
{
    internal enum JobAState
    {
        Begin,
        InProgress,
        Worker1_InProgress,
        Worker1_Complete,
        Worker1_Error,
        Worker2_InProgress,
        Worker2_Complete,
        Worker2_Error,
        Done
    }
}
