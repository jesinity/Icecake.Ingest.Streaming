#if NETSTANDARD2_0

namespace System.Runtime.CompilerServices
{
    // Used by 'required' keyword
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
    public sealed class RequiredMemberAttribute : Attribute
    {
        public RequiredMemberAttribute()
        {
        }
    }

    // Used by the compiler for constructors that set required members
    [AttributeUsage(AttributeTargets.Constructor | AttributeTargets.Method, Inherited = false)]
    public sealed class SetsRequiredMembersAttribute : Attribute
    {
        public SetsRequiredMembersAttribute()
        {
        }
    }

    // Used by 'init' accessors
    public static class IsExternalInit
    {
    }

    // Used by the compiler to mark features like 'required members'
    [AttributeUsage(AttributeTargets.All, Inherited = false, AllowMultiple = true)]
    public sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        public CompilerFeatureRequiredAttribute(string featureName)
        {
            FeatureName = featureName;
        }

        public string FeatureName { get; }

        public string? Language { get; set; }

        // Optional well-known feature names (match real type for completeness)
        public const string RefStructs = "RefStructs";
        public const string RequiredMembers = "RequiredMembers";
    }
}

#endif