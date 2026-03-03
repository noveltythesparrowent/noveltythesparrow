const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const { validationResult } = require('express-validator');

// Security middleware
// Helmet configuration with conditional HSTS (disable on localhost/dev)
const isLocalhost = process.env.NODE_ENV !== 'production';

exports.securityHeaders = helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'", 'https://cdn.jsdelivr.net', 'https://cdnjs.cloudflare.com', 'https://unpkg.com'],
            scriptSrcAttr: ["'unsafe-inline'"],
            styleSrc: ["'self'", "'unsafe-inline'", 'https://cdn.jsdelivr.net', 'https://cdnjs.cloudflare.com'],
            imgSrc: ["'self'", "data:", "https:", "blob:"],
            connectSrc: ["'self'", 'https://api.mtn.com', 'https://cdn.jsdelivr.net', 'https://cdnjs.cloudflare.com'],
            fontSrc: ["'self'", 'https://cdnjs.cloudflare.com'],
            workerSrc: ["'self'", "blob:"]
        }
    },
    // only send HSTS headers in production environments
    hsts: isLocalhost
        ? false
        : {
              maxAge: 31536000,
              includeSubDomains: true,
              preload: true
          },
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginEmbedderPolicy: false
});

// Rate limiting
exports.limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 1000, // limit each IP to 1000 requests per windowMs
    message: 'Too many requests from this IP, please try again after 15 minutes'
});

// Input validation
exports.validateInput = (req, res, next) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
        return res.status(400).json({ 
            success: false,
            errors: errors.array() 
        });
    }
    next();
};