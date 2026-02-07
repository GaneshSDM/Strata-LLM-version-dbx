export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        border: "hsl(var(--border))",
        input: "hsl(var(--input))",
        ring: "hsl(var(--ring))",
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        primary: {
          DEFAULT: "#085690",
          50: "#e6f0f7",
          100: "#cce1ef",
          200: "#99c3df",
          300: "#66a5cf",
          400: "#3387bf",
          500: "#085690",
          600: "#064573",
          700: "#053456",
          800: "#03233a",
          900: "#02111d",
          foreground: "#ffffff",
        },
        accent: {
          DEFAULT: "#ec6225",
          50: "#fef3ed",
          100: "#fde7db",
          200: "#fbcfb7",
          300: "#f9b793",
          400: "#f79f6f",
          500: "#ec6225",
          600: "#bd4e1e",
          700: "#8e3b16",
          800: "#5e270f",
          900: "#2f1407",
          foreground: "#ffffff",
        },
        muted: {
          DEFAULT: "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
      },
      borderRadius: {
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
      },
      boxShadow: {
        'glass': '0 8px 32px 0 rgba(8, 86, 144, 0.1)',
        'glass-lg': '0 12px 48px 0 rgba(8, 86, 144, 0.15)',
        'accent': '0 4px 20px 0 rgba(236, 98, 37, 0.2)',
        'accent-lg': '0 8px 32px 0 rgba(236, 98, 37, 0.25)',
      },
      backgroundImage: {
        'gradient-primary': 'linear-gradient(135deg, #085690 0%, #0a6ba8 100%)',
        'gradient-accent': 'linear-gradient(135deg, #ec6225 0%, #ff7a3d 100%)',
        'gradient-aurora': 'linear-gradient(135deg, #085690 0%, #ec6225 100%)',
        'gradient-aurora-soft': 'linear-gradient(135deg, rgba(8, 86, 144, 0.1) 0%, rgba(236, 98, 37, 0.1) 100%)',
        'mesh-pattern': "url(\"data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23085690' fill-opacity='0.03'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E\")",
      },
      animation: {
        'fade-in': 'fadeIn 0.5s ease-in-out',
        'slide-up': 'slideUp 0.5s ease-out',
        'slide-down': 'slideDown 0.5s ease-out',
        'scale-in': 'scaleIn 0.3s ease-out',
        'pulse-soft': 'pulseSoft 2s ease-in-out infinite',
        'float': 'float 3s ease-in-out infinite',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%': { transform: 'translateY(20px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        slideDown: {
          '0%': { transform: 'translateY(-20px)', opacity: '0' },
          '100%': { transform: 'translateY(0)', opacity: '1' },
        },
        scaleIn: {
          '0%': { transform: 'scale(0.9)', opacity: '0' },
          '100%': { transform: 'scale(1)', opacity: '1' },
        },
        pulseSoft: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.8' },
        },
        float: {
          '0%, 100%': { transform: 'translateY(0px)' },
          '50%': { transform: 'translateY(-10px)' },
        },
      },
      backdropBlur: {
        xs: '2px',
      },
    },
  },
  plugins: [],
}