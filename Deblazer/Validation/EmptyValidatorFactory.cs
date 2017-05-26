using System;

namespace Dg.Deblazer.Validation
{
    internal class EmptyValidatorFactory : IDbEntityValidatorFactory
    {
        public static readonly EmptyValidatorFactory Instance = new EmptyValidatorFactory();

        private EmptyValidatorFactory()
        {
        }

        public IDbEntityValidator GetValidator(Type key) => EmptyValidator.Instance;
    }
}