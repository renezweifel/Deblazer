using System;

namespace Dg.Deblazer.Validation
{
    public interface IDbEntityValidatorFactory
    {
        IDbEntityValidator GetValidator(Type key);
    }
}