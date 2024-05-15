# Online Shopping Platform using gRPC

## Description

This Online Shopping Platforme is a gRPC-based system that facilitates buying and selling operations between sellers and buyers. It consists of a server-side application `market.py` and multiple client-side applications `seller.py`, `buyer.py`. The service allows sellers to register, list, update, and delete items for sale, while buyers can search for items, buy items, add items to their wishlist, and rate items.

## Files Overview

- **market.proto**: Defines the protocol buffer messages and RPC services used in the Market Service.
- **market.py**: Implements the server-side application for the Market Service, providing functionalities for sellers and buyers.
- **seller.py**: Implements the seller client application, allowing sellers to interact with the Market Service.
- **buyer.py**: Implements the buyer client application, allowing buyers to interact with the Market Service.
- **notificationService.proto**: Defines the protocol buffer messages and RPC services for notification handling between the Market Service and clients.

## How to Run

1. **Install Dependencies**:
   - Install gRPC and protobuf dependencies:
     ```
     pip install grpcio grpcio-tools
     ```

2. **Compile Proto Files**:
   - To Compile `market.proto`:
     ```
     python -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/market.proto
     ```

   - To Compile `notificationService.proto`:
     ```
     python -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/notificationService.proto
     ```

3. **Run the Market Service Server**:
   - Execute `market.py` to start the Market Service server:
     ```
     python market.py
     ```

4. **Run Seller Client**:
   - Execute `seller.py` with the Market Service server's IP address and port as well as the seller server's IP address and port:
     ```
     python seller.py <MarketServerIP:Port> <SellerServerIP:Port>
     ```

5. **Run Buyer Client**:
   - Execute `buyer.py` with the Market Service server's IP address and port as well as the buyer server's IP address and port:
     ```
     python buyer.py <MarketServerIP:Port> <BuyerServerIP:Port>
     ```

## Functionality

- **Register Seller**: Sellers can register with the marketplace to start selling items.
- **Sell Item**: Sellers can list items for sale, including product details and pricing.
- **Update Item**: Sellers can update item information such as price and quantity.
- **Delete Item**: Sellers can remove listed items from the marketplace.
- **Search Item**: Buyers can search for items based on name and category.
- **Buy Item**: Buyers can purchase items listed in the marketplace.
- **Add to Wishlist**: Buyers can add items to their wishlist for getting updates.
- **Rate Item**: Buyers can rate items.

## Notification Handling

- The Market Service handles notifications between sellers and buyers using the `notificationService.proto` protocol. Sellers are notified of item purchases, and buyers are notified of item updates.
- For Sending Notifications to Buyer/Seller, Market will act as a Client and Buyer/Seller will act as a Server.
