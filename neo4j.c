// Compile this as
// zcc +zx -vn -O2 -I<Spectranet libraries>/include -L<Spectranet libraries>/lib -o neo4j.bin neo4j.c -lndos -llibsocket -create-app
//
// See https://medium.com/@jorallan/running-neo4j-on-a-zx-spectrum-afa7e128984d for more details

#include <errno.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BUFFER_SIZE 512

#define MESSAGE_HELLO       0x01
#define MESSAGE_RESET       0x0F
#define MESSAGE_RUN         0x10
#define MESSAGE_PULL        0x3F
#define MESSAGE_SUCCESS     0x70
#define MESSAGE_RECORD      0x71

#define min(a,b) (((a)<(b))?(a):(b))
#define set_byte(m,b) ((*(m++))=(b))
#define message_length(m) ((m)-message_send)

unsigned char send_buffer[BUFFER_SIZE]; // The send_buffer is also used to store the full message received
unsigned char receive_buffer[BUFFER_SIZE];
unsigned char* message_send = &send_buffer[2]; // A pointer to where to write the message to send
unsigned char* message_receive = &send_buffer[0]; // A pointer to where to find the message read
int response_length = 0;
int read_pointer = 0;

char tcp_buffer[BUFFER_SIZE];

// Outputs an error code and exits
void message_exit(char* msg)
{
    printf("- %s\n", msg);
    exit(EXIT_FAILURE);
}

// Exit due to a com failure
void error_exit()
{
    // TODO: get strerror() working
    printf("- error code %d\n", errno);
    exit(EXIT_FAILURE);
}

void tcp_send(int fd, unsigned char *buf, int len)
{
    if (send(fd, buf, len, 0) < 0)
    {
	printf("Error sending\n");
	error_exit();
    }
}

// The callback for tcp_connect() that handles incoming bytes and writes them to com_buffer
void read_callback(const char* data, int16_t len)
{
    if (response_length < BUFFER_SIZE)
    {
        if (response_length + len > BUFFER_SIZE)
        {
            len = BUFFER_SIZE - response_length;
        }
        memcpy(receive_buffer + response_length, data, len);
        response_length += len;
    }
}

// Blocking read that reads len bytes into receive_buffer
void tcp_read(int fd, int16_t len)
{
    int r;

    // We don't have a check to see if len is too long, since we do that in bolt_receive()
    
    // Shift the buffer down if we had already read part of it before
    if (read_pointer > 0)
    {
        response_length -= read_pointer;
        // memcpy of overlapping arrays is undefined behavior in C,
        // but this complier handles backward overlaps well
        memcpy(receive_buffer, &receive_buffer[read_pointer], response_length);
    }
    
    while (response_length < len)
    {
        r = recv(fd, tcp_buffer, sizeof(tcp_buffer), 0);
        if (r < 0)
	{
	    printf("Error from recv()\n");
            error_exit();
        }
	read_callback(tcp_buffer, r);
    }
    read_pointer = len;
}

// Clears the read buffer
void clear_buffer()
{
    response_length = 0;
    read_pointer = 0;
}

// Parses a big-endian 16 bit integer from a byte array
uint16_t parseInt16(unsigned char* data)
{
    return (uint16_t)data[0] << 8 | data[1];
}

// Parses a big-endian 32 bit integer from a byte array
uint32_t parseInt32(unsigned char* data)
{
    return (uint32_t)data[0] << 24 | (uint32_t)data[1] << 16 | (uint32_t)data[2] << 8 | data[3];
}

// Prints the content of a record on standard out
void print_record(int len)
{
    int l, i;
    char memory, t;
    char* structure;
    unsigned char* message = message_receive;
    
    printf("-");
    
    while (message < message_receive + len)
    {
        unsigned char marker = *(message++);
        
        // NULL
        if (marker == 0xC0)
        {
            printf(" NULL");
        }
        
        // TINY INT
        else if (marker >= 0xF0 || marker <= 0x7F)
        {
            printf(" %d", (int)marker);
        }
        
        // INT 8
        else if (marker == 0xC8)
        {
            printf(" %d", (int)(*(message++)));
        }
        
        // INT 16
        else if (marker == 0xC9)
        {
            printf(" %d", parseInt16(message));
            message += sizeof(uint16_t);
        }
        
        // INT 32
        else if (marker == 0xCA)
        {
            printf(" %d", parseInt32(message));
            message += sizeof(int32_t);
        }
        
        // INT 64
        else if (marker == 0xCB)
        {
            // Not supported
            printf(" INT64");
            message += 8;
        }
        
        // FLOAT
        else if (marker == 0xC1)
        {
            // Not supported
            printf(" FLOAT");
            message += 8;
        }
        
        // BYTE ARRAY
        else if (marker >= 0xCC && marker <= 0xCE)
        {
            if (marker == 0xCC)
            {
                l = (int)(*(message++));
            }
            else if (marker == 0xCD)
            {
                l = parseInt16(message);
                message += sizeof(uint16_t);
            }
            else
            {
                l = parseInt32(message);
                message += sizeof(uint32_t);
            }
            printf(" [");
            for(i = 0; i < l; i++)
            {
                if (i > 0)
                {
                    printf(",");
                }
                printf("%02X", (*(message++)));
            }
            printf("]");
        }
        
        // STRING
        else if ((marker >= 0x80 && marker <= 0x8F) || (marker >= 0xD0 && marker <= 0xD2))
        {
            if (marker >= 0x80 && marker <= 0x8F)
            {
                l = marker - 0x80;
            }
            else if (marker == 0xD0)
            {
                l = (int)(*(message++));
            }
            else if (marker == 0xD1)
            {
                l = parseInt16(message);
                message += sizeof(uint16_t);
            }
            else
            {
                l = parseInt32(message);
                message += sizeof(uint32_t);
            }
            memory = message[l];
            // This could fall beyond the message length, but we assume that the whole
            // message buffer isn't used
            message[l] = '\0';
            printf(" %s", message);
            message[l] = memory;
            message += l;
        }
        
        // LIST
        else if ((marker >= 0x90 && marker <= 0x9F) || (marker >= 0xD4 && marker <= 0xD6))
        {
            if (marker >= 0x90 && marker <= 0x9F)
            {
                l = marker - 0x90;
            }
            else if (marker == 0xD4)
            {
                l = (int)(*(message++));
            }
            else if (marker == 0xD5)
            {
                l = parseInt16(message);
                message += sizeof(uint16_t);
            }
            else
            {
                l = parseInt32(message);
                message += sizeof(uint32_t);
            }
            // In our crude parsing we don't format the lists as lists, instead we write that
            // a list is started, and then the elements are printed as base values
            printf(" L(%d)", l);
        }
        
        // DICTIONARY
        else if ((marker >= 0xA0 && marker <= 0xAF) || (marker >= 0xD8 && marker <= 0xDA))
        {
            if (marker >= 0xA0 && marker <= 0xAF)
            {
                l = marker - 0xA0;
            }
            else if (marker == 0xD8)
            {
                l = (int)(*(message++));
            }
            else if (marker == 0xD9)
            {
                l = parseInt16(message);
                message += sizeof(uint16_t);
            }
            else
            {
                l = parseInt32(message);
                message += sizeof(uint32_t);
            }
            // In our crude parsing we don't format the dictionaries as dictionaries, instead we write that
            // a dictionary is started, and then the keys and values are printed as base values
            printf(" D(%d)", l);
        }
        
        // STRUCTURE
        else if (marker >= 0xB0 && marker <= 0xBF)
        {
            l = marker - 0xB0;
            t = (*(message++));
            structure = 0;
            switch (t)
            {
                case 0x4E: structure = "Node"; break;
                case 0x52: structure = "Relationship"; break;
                case 0x72: structure = "UnboundRelationship"; break;
                case 0x50: structure = "Path"; break;
                case 0x44: structure = "Date"; break;
                case 0x54: structure = "Time"; break;
                case 0x74: structure = "LocalTime"; break;
                case 0x49: structure = "DateTime"; break;
                case 0x69: structure = "DateTimeZoneId"; break;
                case 0x64: structure = "LocalDateTime"; break;
                case 0x45: structure = "Duration"; break;
                case 0x58: structure = "Point2D"; break;
                case 0x59: structure = "Point3D"; break;
                case 0x70: structure = "SUCCESS"; break;
                case 0x7E: structure = "IGNORED"; break;
                case 0x7F: structure = "FAILURE"; break;
                case 0x71: structure = "RECORD"; break;
            }
            if (structure == 0)
            {
                printf(" S(%02X, %d)", t, l);
            }
            else
            {
                printf(" S(%s, %d)", structure, l);
            }
        }
    }
    printf("\n");
}

// Packs a bolt message contained in message_buffer into a chunk and sends it
void bolt_send(int fd, int16_t len)
{
    // Since our buffer is limited to 512 bytes, we know we never have to use
    // more than one chunk, and we also know that the most significant byte of
    // the length is always 0
    if(len > BUFFER_SIZE - 4)
    {
        message_exit("Message too long");
    }
    
    // Header
    send_buffer[0] = 0;
    send_buffer[1] = len;
    
    // Message end
    send_buffer[len+2] = 0;
    send_buffer[len+3] = 0;
    
    // Send chunk
    tcp_send(fd, send_buffer, len+4);
}

// Reads back a bolt reply and returns the length of it
// The message read can be found at message_receive
int bolt_receive(int fd)
{
    // Since we can't handle messages longer than 512 bytes we shouldn't have to care about
    // chunking when receiving either, but theoretically the server could chunk a shorter message,
    // so we will. And to manage that we reuse the send_buffer to unpack the received message into
    int l;
    int message_reply_length = 0;
    
    while (1)
    {
        tcp_read(fd, 2);
        l = parseInt16(receive_buffer);
        if (l == 0)
        {
            break;
        }
        if((l + message_reply_length) > BUFFER_SIZE)
        {
            message_exit("Reply too long");
        }
        tcp_read(fd, l);
        memcpy(send_buffer + message_reply_length, receive_buffer, l);
        message_reply_length += l;
    }
    
    return message_reply_length;
}

// Packs a bolt message contained in message_buffer into a chunk and sends it,
// and then reads back a bolt reply and returns the length of it
// The message read can be found at message_receive
int bolt_send_receive(int fd, int16_t len)
{
    clear_buffer();
    
    bolt_send(fd, len);
    
    return bolt_receive(fd);
}

// Note that the functions below doesn't have any check to avoid the message growing
// to big and spanning beyond the buffer. That has to be managed by the caller.

// Starts a struct with a defined number of fields
// You need to manually add each field after this call
unsigned char* start_struct(unsigned char* message, char signature, char fields)
{
    set_byte(message, 0xB0 + fields);
    set_byte(message, signature);
    return message;
}

// Starts a dictionary for a certain number of fields
// You need to manually add each key pair after this call
// (currently doesn't support dictionaries with more than 15 fields)
unsigned char* start_dictionary(unsigned char* message, char fields)
{
    set_byte(message, 0xA0 + fields);
    return message;
}

// Adds an int to the message
// Currently we only support tiny ints (-16 - 127) because
// it is all we currenlty use
unsigned char* add_int(unsigned char* message, int value)
{
    if (value >= -16 && value <= 127)
    {
        set_byte(message, (char)value);
    }
    return message;
}

// Adds a string to the message (currently only supports up to 255 character strings)
unsigned char* add_string(unsigned char* message, char* value)
{
    // Bolt strings are not null-terminated, so we don't include the null terminator here
    int len = strlen(value);
    
    if (len < 16)
    {
        set_byte(message, 0x80 + len);
    }
    else
    {
        set_byte(message, 0xD0);
        set_byte(message, len);
    }
    memcpy(message, value, len);
    return message+len;
}

// Adds an int key pair to a dictionary
// You first have to call start_dictionary()
unsigned char* add_keypair_int(unsigned char* message, char* key, int value)
{
    message = add_string(message, key);
    message = add_int(message, value);
    return message;
}

// Adds a string key pair to a dictionary
// You first have to call start_dictionary()
unsigned char* add_keypair_string(unsigned char* message, char* key, char* value)
{
    message = add_string(message, key);
    message = add_string(message, value);
    return message;
}

// Adds a dictionary key pair to a dictionary
// You first have to call start_dictionary()
unsigned char* add_keypair_dictionary(unsigned char* message, char* key, int value_fields)
{
    message = add_string(message, key);
    message = start_dictionary(message, value_fields);
    return message;
}

// Sends a Reset message (needed when the server is in a failed state)
int send_reset(int fd)
{
    unsigned char* message = message_send;
    message = start_struct(message, MESSAGE_RESET, 0);
    
    bolt_send_receive(fd, message_length(message));
    
    return message_receive[0] == 0xB1 && message_receive[1] == MESSAGE_SUCCESS;
}

// Send the Hello message.
void send_hello(int fd, char* user_name, char* password)
{
    int len;
    unsigned char* message = message_send;
    message = start_struct(message, MESSAGE_HELLO, 1);
    message = start_dictionary(message, 4); // The main ("extra") dictionary
    
    message = add_keypair_string(message, "scheme", "basic");         // Part of extra
    message = add_keypair_string(message, "principal", user_name);    // Part of extra
    message = add_keypair_string(message, "credentials", password);   // Part of extra
    message = add_keypair_string(message, "user_agent", "Spectranet/1.0.0"); // Part of extra
    
    len = bolt_send_receive(fd, message_length(message));
    
    if (message_receive[0] != 0xB1 || message_receive[1] != MESSAGE_SUCCESS)
    {
        print_record(len);
        message_exit("Authentication failure\n");
    }
}

void unescape_query(char* query)
{
    int state = 0;
    char* src = query;
    char* dest = src;

    while (*src)
    {
        switch (state)
        {
	    case 0:
	        if (*src == '/')
	        {
	            src++;
		    state = 1;
	        }
	        else
                {
		    *dest++ = *src++;
	        }
	        break;

	    case 1:
	        switch (*src)
 	        {
		    case '(': *dest++ = '['; src++; break;
		    case ')': *dest++ = ']'; src++; break;
		    case '<': *dest++ = '{'; src++; break;
		    case '>': *dest++ = '}'; src++; break;
		    default: *dest++ = *src++;
	        }
	        state = 0;
	        break;
	}
    }

    *dest = 0;
}

// Executes an auto-commit query (currently doesn't support parameters)
// Result is printed on standard out
void execute_query(int fd, char* query)
{
    int len;
    unsigned char* message = message_send;

    unescape_query(query);
    printf("Unescaped query: %s\n", query);

    if (strlen(query) > BUFFER_SIZE - 8)
    {
        printf("Query too long, cannot be sent\n");
        return;
    }
    
    message = start_struct(message, MESSAGE_RUN, 3);
    message = add_string(message, query);
    message = start_dictionary(message, 0); // The "parameters" dictionary
    message = start_dictionary(message, 0); // The "extra" dictionary
    
    len = bolt_send_receive(fd, message_length(message));
    
    if (message_receive[0] != 0xB1 || message_receive[1] != MESSAGE_SUCCESS)
    {
        printf("Query execution failure\n");
        print_record(len);
        while (!send_reset(fd))
        {
            printf("Retrying reset...");
        }
        return;
    }
    
    message = message_send;
    message = start_struct(message, MESSAGE_PULL, 1);
    message = start_dictionary(message, 1); // The main ("extra") dictionary
    message = add_keypair_int(message, "n", -1); // Part of extra
    
    len = bolt_send_receive(fd, message_length(message));
    
    while (message_receive[0] == 0xB1 && message_receive[1] == MESSAGE_RECORD)
    {
        print_record(len);
        len = bolt_receive(fd);
    }
}

void input(char* destination, int limit)
{
    int c, i;
    i = 0;
    while (i < limit)
    {
        c = getchar();
        if (c == 0x0A) // ENTER - accept
        {
            break;
        }
        else
        {
            destination[i++] = c;
        }
    }
    destination[i] = 0;
}

int connect_to_server(char *hostname, int port)
{
    struct hostent *he;
    int sockfd;
    struct sockaddr_in remoteaddr;

    he = gethostbyname(hostname);
    if (!he)
    {
	printf("Failed to look up remote host %s\n", hostname);
	error_exit();
    }

    printf("Creating socket\n");
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
	printf("Couldn't open socket; rc=%d\n", sockfd);
	error_exit();
    }
    
    printf("Connecting\n");
    remoteaddr.sin_family = AF_INET;
    remoteaddr.sin_port = htons(port);
    remoteaddr.sin_addr.s_addr = he->h_addr;
    if(connect(sockfd, (struct sockaddr*)&remoteaddr, sizeof(struct sockaddr_in)) < 0)
    {
	close(sockfd);
	printf("Connect failed!\n");
	error_exit();
    }

    return sockfd;
}

void handshake(int fd)
{
    // This includes both the initial handshake and the version exchange
    // (we only implement protocol version 5.0,
    // which should be supported by all Neo4j 5.x versions)
    unsigned char handshake[] = {0x60, 0x60, 0xB0, 0x17, 0x00, 0x00, 0x00, 0x05, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    
    tcp_send(fd, handshake, sizeof(handshake));
    
    // Wait for the 4 byte reply and validate it
    tcp_read(fd, 4);
    if (receive_buffer[3] != 0x05)
    {
        message_exit("Server version not supported");
    }
}    

int main(int argc, char* argv[])
{
    // The command we are executing
    char command[64];
    
    // Server details
    char* hostname = "your-neo4j-instance.example.com";
    char* user = "neo4j";
    char* password = "SuperSecret";
    int port = 7687;

    int sockfd;
    
    printf("Using hostname %s, user %s, password (...) and port %d\n", hostname, user, port);

    sockfd = connect_to_server(hostname, port);

    handshake(sockfd);
    
    // Authenticate with the hello command
    send_hello(sockfd, user, password);
    
    // Main loop of user entering queries that we execute
    printf("Connected.\n");
    while (1)
    {
        printf("Enter your query:\n");
        input(command, 63);
        execute_query(sockfd, command);
    }

    close(sockfd);

    return 0;
}
