INSERT INTO template (id, code, channel, subject, body, version, active) VALUES (
    'b3d6f0a2-0000-4000-8000-000000000001', 'welcome-email', 'EMAIL',
    'Welcome to Acme, {{name}}!',
    '<h1>Hi {{name}},</h1><p>Thanks for signing up. Your account <b>{{email}}</b> is ready.</p>',
    1, 1);

INSERT INTO template (id, code, channel, subject, body, version, active) VALUES (
    'b3d6f0a2-0000-4000-8000-000000000002', 'otp-sms', 'SMS',
    NULL,
    'Your Acme verification code is {{code}}. It expires in {{minutes}} minutes.',
    1, 1);

INSERT INTO template (id, code, channel, subject, body, version, active) VALUES (
    'b3d6f0a2-0000-4000-8000-000000000003', 'order-shipped-push', 'PUSH',
    'Order {{orderId}} shipped',
    'Your order {{orderId}} is on its way and should arrive by {{eta}}.',
    1, 1);
