import { useState, useEffect } from 'react';

const ITEMS = ['PIZZA', 'COFFEE', 'DIET_COKE'];
const getStatusBadgeStyle = (status) => {
  switch (status) {
    case 'CREATED':
      return {
        background: '#dbeafe', // Light blue
        color: '#1e40af',      // Dark blue text
        border: '1px solid #bfdbfe',
      };
    case 'COOKING':
      return {
        background: '#fef3c7', // Warm yellow/amber
        color: '#92400e',      // Amber text
        border: '1px solid #fde68a',
      };
    case 'COMPLETED':
      return {
        background: '#dcfce7', // Fresh green
        color: '#166534',      // Dark green text
        border: '1px solid #bbf7d0',
      };
    default:
      return {
        background: '#f4f4f5',
        color: '#52525b',
        border: '1px solid #e4e4e7',
      };
  }
};
export default function App() {
  const [selectedItems, setSelectedItems] = useState([]);
  const [orders, setOrders] = useState([]);

  // Toggle item selection for current order
  const toggleItem = (item) => {
    setSelectedItems((prev) =>
      prev.includes(item) ? prev.filter((i) => i !== item) : [...prev, item]
    );
  };

  // Submit order to Go API
  const placeOrder = async () => {
    if (selectedItems.length === 0) return;

    try {
      const res = await fetch('http://localhost:8080/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: selectedItems }),
      });

      if (res.ok) {
        setSelectedItems([]);
      }
    } catch (err) {
      console.error('Failed to place order:', err);
    }
  };

  useEffect(() => {
  const eventSource = new EventSource('http://localhost:8080/orders/stream');

  eventSource.onopen = () => {
    console.log('🟢 SSE Stream connected to Go backend!');
  };

  eventSource.onmessage = (event) => {
    const updatedOrder = JSON.parse(event.data);

    setOrders((prevOrders) => {
      const exists = prevOrders.some((o) => o.id === updatedOrder.id);
      if (exists) {
        // Update status of existing card
        return prevOrders.map((o) => (o.id === updatedOrder.id ? updatedOrder : o));
      }
      // New order card
      return [updatedOrder, ...prevOrders];
    });
  };

  eventSource.onerror = (err) => {
    console.error('🔴 SSE Connection error:', err);
  };

  return () => {
    eventSource.close();
  };
}, []);

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', maxWidth: '900px', margin: '2rem auto', padding: '0 1rem' }}>
      <h1>☕ Cafe Engine Dashboard</h1>

      {/* POS Menu Section */}
      <div style={{ background: '#f4f4f5', padding: '1.5rem', borderRadius: '8px', marginBottom: '2rem' }}>
        <h2>1. Place New Order</h2>
        <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
          {ITEMS.map((item) => {
            const isSelected = selectedItems.includes(item);
            return (
              <button
                key={item}
                onClick={() => toggleItem(item)}
                style={{
                  padding: '0.75rem 1.5rem',
                  borderRadius: '6px',
                  border: '2px solid #18181b',
                  fontWeight: 'bold',
                  cursor: 'pointer',
                  background: isSelected ? '#18181b' : '#fff',
                  color: isSelected ? '#fff' : '#18181b',
                }}
              >
                {item}
              </button>
            );
          })}
        </div>

        <button
          onClick={placeOrder}
          disabled={selectedItems.length === 0}
          style={{
            padding: '0.75rem 2rem',
            background: selectedItems.length > 0 ? '#22c55e' : '#a1a1aa',
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            fontSize: '1rem',
            fontWeight: 'bold',
            cursor: selectedItems.length > 0 ? 'pointer' : 'not-allowed',
          }}
        >
          Submit Order ({selectedItems.length} items)
        </button>
      </div>

      {/* Live Orders Stream Section */}
      <div>
        <h2>2. Live Orders (Real-Time SSE Stream)</h2>
        {orders.length === 0 ? (
          <p style={{ color: '#71717a' }}>No orders placed yet. Choose items above and hit Submit!</p>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
           {orders.map((order) => {
  const badgeStyle = getStatusBadgeStyle(order.status);

  return (
    <div
      key={order.id}
      style={{
        border: '1px solid #e4e4e7',
        borderRadius: '8px',
        padding: '1rem',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        background: '#fff',
        boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
        transition: 'all 0.3s ease',
      }}
    >
      <div>
        <strong>Order ID:</strong>{' '}
        <code style={{  padding: '0.2rem 0.4rem', borderRadius: '4px' }}>
          {order.id}
        </code>
        <div style={{ color: '#52525b', marginTop: '0.25rem' }}>
          Items: {order.items ? order.items.join(', ') : ''}
        </div>
      </div>

      {/* Dynamic Colored Status Badge */}
      <span
        style={{
          ...badgeStyle,
          padding: '0.4rem 0.85rem',
          borderRadius: '999px',
          fontSize: '0.85rem',
          fontWeight: 'bold',
          letterSpacing: '0.025em',
          transition: 'all 0.3s ease',
        }}
      >
        {order.status === 'COOKING' ? '🔥 IN PROGRESS' : order.status === 'COMPLETED' ? '✅ COMPLETED' : '⏳ CREATED'}
      </span>
    </div>
  );
})}
          </div>
        )}
      </div>
    </div>
  );
}