#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/system/error_code.hpp>

#include <iostream>
#include <iomanip>
#include <string>

using tcp = boost::asio::ip::tcp;
namespace websocket = boost::beast::websocket;

void Log(const std::string& where, boost::system::error_code ec) {
    std::cerr << "[" << std::setw(20) << where << "] "
              << (ec ? "Error: " : "OK")
              << (ec ? ec.message() : "")
              << std::endl;
}

void OnReceive
(
  boost::beast::flat_buffer& rBuffer,

  const boost::system::error_code& ec
)
{
  if (ec) {
    Log("OnReceive", ec);
    return;
  }

  // Print the echod message.
    std::cout << "ECHO: " 
              << boost::beast::make_printable(rBuffer.data())
              << std::endl;
}

void OnSend
(
  websocket::stream<boost::beast::tcp_stream>& ws,
  boost::beast::flat_buffer& rBuffer,

  const boost::system::error_code& ec
) 
{
  if(ec)
  {
    Log("OnSend", ec);
    return;
  }

  // Read the echoed message back.
  ws.async_read(rBuffer, [&rBuffer](auto ec, auto nBytesRead){
      OnReceive(rBuffer, ec);
  }); 
}

void OnHandshake
(
  websocket::stream<boost::beast::tcp_stream>& ws,
  boost::asio::const_buffer& wBuffer,
  boost::beast::flat_buffer& rBuffer,

  const boost::system::error_code& ec
)
{
  if(ec)
  {
    Log("OnHandshake", ec);
    return;
  }

  // Tell the WebSocket object to exchange messages in text format.
  ws.text(true);

  // Send a message to the connected WebSocket server.
  ws.async_write(wBuffer, [&ws, &rBuffer](auto ec, auto nBytesWritten) {
    OnSend(ws, rBuffer, ec);
  });
}

void OnConnect
(
  websocket::stream<boost::beast::tcp_stream>& ws,
  const std::string& url,
  const std::string& endpoint,
  boost::asio::const_buffer& wBuffer,
  boost::beast::flat_buffer& rBuffer,

  const boost::system::error_code& ec
) 
{
  if(ec)
  {
    Log("OnConnect", ec);
    return;
  }

  // Attempt a WebSocket handshake.
  ws.async_handshake(url, endpoint, [&ws, &wBuffer, &rBuffer](auto ec) {
    OnHandshake(ws, wBuffer, rBuffer, ec);
  });
}

void OnResolve
(
  websocket::stream<boost::beast::tcp_stream>& ws,
  const std::string& url,
  const std::string& endpoint,
  boost::asio::const_buffer& wBuffer,
  boost::beast::flat_buffer& rBuffer,

  const boost::system::error_code& ec,
  tcp::resolver::iterator resolverIt
) 
{
  if(ec)
  {
    Log("OnResolve", ec);
    return;
  }

  // Connect to the TCP socket.
  // Instead of constructing the socket and the ws objects separately, the
  // socket is now embedded in ws, and we access it through next_layer().
  ws.next_layer().async_connect(*resolverIt, [&ws, &url, &endpoint, &wBuffer, &rBuffer](auto ec){
    OnConnect(ws, url, endpoint, wBuffer, rBuffer, ec);
  });
}

int main() {
      // Connection targets
    const std::string url {"ltnm.learncppthroughprojects.com"};
    const std::string endpoint {"/echo"};
    const std::string port {"80"};
    const std::string message {"Hello WebSocket"};

    // Always start with an I/O context object.
    boost::asio::io_context ioc {};

    // Create the objects that will be shared by the connection callbacks.
    websocket::stream<boost::beast::tcp_stream> ws {ioc};
    boost::asio::const_buffer wBuffer {message.c_str(), message.size()};
    boost::beast::flat_buffer rBuffer {};

    // Resolve endpoint.
    tcp::resolver resolver {ioc};
    resolver.async_resolve(url, port, [&ws, &url, &endpoint, &wBuffer, &rBuffer](auto ec, auto resolverIt){
      OnResolve(ws, url, endpoint, wBuffer, rBuffer, ec, resolverIt);
    });

    // We must call io_context::run for asynchronous callbacks to run.
    ioc.run();

    // We only succeed if the buffer contains the same message we sent out
    // originally.
    if (boost::beast::buffers_to_string(rBuffer.data()) != message) {
        return -1;
    }

    return 0;
}