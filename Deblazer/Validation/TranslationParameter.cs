
namespace Dg.Deblazer.Validation
{
    public class TranslationParameter
    {
        public TranslationParameter(object parameter, ParameterTranslationMode parameterTranslationMode = ParameterTranslationMode.DoNotTranslate)
        {
            ParameterTranslationMode = parameterTranslationMode;
            Parameter = parameter;
        }

        public ParameterTranslationMode ParameterTranslationMode { get; }
        public object Parameter { get; }
    }
}
