module Aws.DynamoDb.Core where

import           Aws.Core
import           Control.Arrow                  ((***))
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Aeson                     ((.=))
import           Data.Aeson.Types               (Pair)
import           Data.Attempt                   (Attempt(..))
import           Data.Conduit                   (($$+-))
import           Data.Function
import           Data.IORef
import           Data.List
import           Data.Maybe
import           Data.Monoid
import           Data.Time
import           Data.Typeable
import           System.Locale
import           Text.XML.Cursor                (($/), (&|))
import qualified Blaze.ByteString.Builder       as Blaze
import qualified Blaze.ByteString.Builder.Char8 as Blaze8
import qualified Control.Exception              as C
import qualified Control.Failure                as F
import qualified Crypto.Hash.MD5                as MD5
import qualified Data.ByteString                as B
import qualified Data.ByteString.Char8          as B8
import qualified Data.ByteString.Base64         as Base64
import qualified Data.CaseInsensitive           as CI
import qualified Data.Conduit                   as C
import qualified Data.Serialize                 as Serialize
import qualified Data.Text                      as T
import qualified Data.Text.Encoding             as T
import qualified Network.HTTP.Conduit           as HTTP
import qualified Network.HTTP.Types             as HTTP
import qualified Text.XML                       as XML
import qualified Text.XML.Cursor                as Cu

data DynamoDbAuthorization 
    = DynamoDbAuthorizationHeader 
    | DynamoDbAuthorizationQuery
    deriving (Show)

data DynamoDbConfiguration qt
    = DynamoDbConfiguration {
        ddbProtocol :: Protocol
      , ddbEndpoint :: B.ByteString
      , ddbPort :: Int
      , ddbUseUri :: Bool
      , ddbDefaultExpiry :: NominalDiffTime
      }
    deriving (Show)

instance DefaultServiceConfiguration (DynamoDbConfiguration NormalQuery) where
  defServiceConfig = ddb HTTPS ddbEndpointUsClassic False
  
  debugServiceConfig = ddb HTTP ddbEndpointUsClassic False

instance DefaultServiceConfiguration (DynamoDbConfiguration UriOnlyQuery) where
  defServiceConfig = ddb HTTPS ddbEndpointUsClassic True
  debugServiceConfig = ddb HTTP ddbEndpointUsClassic True

ddbEndpointUsClassic :: B.ByteString
ddbEndpointUsClassic = "ddb.amazonaws.com"

ddbEndpointUsWest :: B.ByteString
ddbEndpointUsWest = "ddb-us-west-1.amazonaws.com"

ddbEndpointUsWest2 :: B.ByteString
ddbEndpointUsWest2 = "ddb-us-west-2.amazonaws.com"

ddbEndpointEu :: B.ByteString
ddbEndpointEu = "ddb-eu-west-1.amazonaws.com"

ddbEndpointApSouthEast :: B.ByteString
ddbEndpointApSouthEast = "ddb-ap-southeast-1.amazonaws.com"

ddbEndpointApNorthEast :: B.ByteString
ddbEndpointApNorthEast = "ddb-ap-northeast-1.amazonaws.com"

ddb :: Protocol -> B.ByteString -> Bool -> DynamoDbConfiguration qt
ddb protocol endpoint uri 
    = DynamoDbConfiguration { 
         ddbProtocol = protocol
       , ddbEndpoint = endpoint
       , ddbPort = defaultPort protocol
       , ddbUseUri = uri
       , ddbDefaultExpiry = 15*60
       }

type ErrorCode = T.Text

data DynamoDbError
    = DynamoDbError {
        ddbStatusCode :: HTTP.Status
      , ddbErrorCode :: ErrorCode -- Error/Code
      , ddbErrorMessage :: T.Text -- Error/Message
      , ddbErrorResource :: Maybe T.Text -- Error/Resource
      , ddbErrorHostId :: Maybe T.Text -- Error/HostId
      , ddbErrorAccessKeyId :: Maybe T.Text -- Error/AWSAccessKeyId
      , ddbErrorStringToSign :: Maybe B.ByteString -- Error/StringToSignBytes (hexadecimal encoding)
      }
    deriving (Show, Typeable)

instance C.Exception DynamoDbError

data DynamoDbMetadata
    = DynamoDbMetadata {
        ddbMAmzId2 :: Maybe T.Text
      , ddbMRequestId :: Maybe T.Text
      }
    deriving (Show, Typeable)

instance Monoid DynamoDbMetadata where
    mempty = DynamoDbMetadata Nothing Nothing
    DynamoDbMetadata a1 r1 `mappend` DynamoDbMetadata a2 r2 = DynamoDbMetadata (a1 `mplus` a2) (r1 `mplus` r2)

instance Loggable DynamoDbMetadata where
    toLogText (DynamoDbMetadata id2 rid) = "DynamoDb: request ID=" `mappend`
                                     fromMaybe "<none>" rid `mappend`
                                     ", x-amz-id-2=" `mappend`
                                     fromMaybe "<none>" id2

data DynamoDbQuery
    = DynamoDbQuery {
        ddbQMethod :: Method
      , ddbQSubresources :: HTTP.Query
      , ddbQQuery :: HTTP.Query
      , ddbQContentType :: Maybe B.ByteString
      , ddbQContentMd5 :: Maybe MD5.MD5
      , ddbQAmzHeaders :: HTTP.RequestHeaders
      , ddbQOtherHeaders :: HTTP.RequestHeaders
      , ddbQRequestBody :: Maybe (HTTP.RequestBody (C.ResourceT IO))
      }

instance Show DynamoDbQuery where
    show DynamoDbQuery{..} = "DynamoDbQuery [" ++
                       " method: " ++ show ddbQMethod ++
                       " ; subresources: " ++ show ddbQSubresources ++
                       " ; query: " ++ show ddbQQuery ++
                       " ; request body: " ++ (case ddbQRequestBody of Nothing -> "no"; _ -> "yes") ++
                       "]"

ddbSignQuery :: DynamoDbQuery -> DynamoDbConfiguration qt -> SignatureData -> SignedQuery
ddbSignQuery DynamoDbQuery{..} DynamoDbConfiguration{..} SignatureData{..}
    = SignedQuery {
        sqMethod = ddbQMethod
      , sqProtocol = ddbProtocol
      , sqHost = B.intercalate "." $ catMaybes host
      , sqPort = ddbPort
      , sqPath = mconcat $ catMaybes path
      , sqQuery = sortedSubresources ++ ddbQQuery ++ authQuery :: HTTP.Query
      , sqDate = Just signatureTime
      , sqAuthorization = authorization
      , sqContentType = ddbQContentType
      , sqContentMd5 = ddbQContentMd5
      , sqAmzHeaders = amzHeaders
      , sqOtherHeaders = ddbQOtherHeaders
      , sqBody = ddbQRequestBody
      , sqStringToSign = stringToSign
      }
    where
      amzHeaders = merge $ sortBy (compare `on` fst) ddbQAmzHeaders
          where merge (x1@(k1,v1):x2@(k2,v2):xs) | k1 == k2  = merge ((k1, B8.intercalate "," [v1, v2]) : xs)
                                                 | otherwise = x1 : merge (x2 : xs)
                merge xs = xs

      -- (host, path) = B.span (/=fromIntegral '/') ddbEndpoint
      (host, path) = ([], [])
      sortedSubresources = sort ddbQSubresources
      canonicalizedResource = Blaze8.fromChar '/' `mappend`
                              HTTP.renderQueryBuilder True sortedSubresources
      ti = case (ddbUseUri, signatureTimeInfo) of
             (False, ti') -> ti'
             (True, AbsoluteTimestamp time) -> AbsoluteExpires $ ddbDefaultExpiry `addUTCTime` time
             (True, AbsoluteExpires time) -> AbsoluteExpires time
      sig = signature signatureCredentials HmacSHA1 stringToSign
      stringToSign = Blaze.toByteString . mconcat . intersperse (Blaze8.fromChar '\n') . concat  $
                       [[Blaze.copyByteString $ httpMethod ddbQMethod]
                       , [maybe mempty (Blaze.copyByteString . Base64.encode . Serialize.encode) ddbQContentMd5]
                       , [maybe mempty Blaze.copyByteString ddbQContentType]
                       , [Blaze.copyByteString $ case ti of
                                                   AbsoluteTimestamp time -> fmtRfc822Time time
                                                   AbsoluteExpires time -> fmtTimeEpochSeconds time]
                       , map amzHeader amzHeaders
                       , [canonicalizedResource]
                       ]
          where amzHeader (k, v) = Blaze.copyByteString (CI.foldedCase k) `mappend` Blaze8.fromChar ':' `mappend` Blaze.copyByteString v
      (authorization, authQuery) = case ti of
                                 AbsoluteTimestamp _ -> (Just $ B.concat ["AWS ", accessKeyID signatureCredentials, ":", sig], [])
                                 AbsoluteExpires time -> (Nothing, HTTP.toQuery $ makeAuthQuery time)
      makeAuthQuery time
          = [("Expires" :: B8.ByteString, fmtTimeEpochSeconds time)
            , ("AWSAccessKeyId", accessKeyID signatureCredentials)
            , ("SignatureMethod", "HmacSHA256")
            , ("Signature", sig)]

ddbResponseConsumer :: HTTPResponseConsumer a
                   -> IORef DynamoDbMetadata
                   -> HTTPResponseConsumer a
ddbResponseConsumer inner metadata resp = do
      let headerString = fmap T.decodeUtf8 . flip lookup (HTTP.responseHeaders resp)
      let amzId2 = headerString "x-amz-id-2"
      let requestId = headerString "x-amz-request-id"

      let m = DynamoDbMetadata { ddbMAmzId2 = amzId2, ddbMRequestId = requestId }
      liftIO $ tellMetadataRef metadata m

      if HTTP.responseStatus resp >= HTTP.status400
        then ddbErrorResponseConsumer resp
        else inner resp

ddbXmlResponseConsumer :: (Cu.Cursor -> Response DynamoDbMetadata a)
                      -> IORef DynamoDbMetadata
                      -> HTTPResponseConsumer a
ddbXmlResponseConsumer parse metadataRef =
    ddbResponseConsumer (xmlCursorConsumer parse metadataRef) metadataRef

ddbBinaryResponseConsumer :: HTTPResponseConsumer a
                         -> IORef DynamoDbMetadata
                         -> HTTPResponseConsumer a
ddbBinaryResponseConsumer inner metadataRef = ddbResponseConsumer inner metadataRef

ddbErrorResponseConsumer :: HTTPResponseConsumer a
ddbErrorResponseConsumer resp
    = do doc <- HTTP.responseBody resp $$+- XML.sinkDoc XML.def
         let cursor = Cu.fromDocument doc
         liftIO $ case parseError cursor of
           Success err      -> C.monadThrow err
           Failure otherErr -> C.monadThrow otherErr
    where
      parseError :: Cu.Cursor -> Attempt DynamoDbError
      parseError root = do code <- force "Missing error Code" $ root $/ elContent "Code"
                           message <- force "Missing error Message" $ root $/ elContent "Message"
                           let resource = listToMaybe $ root $/ elContent "Resource"
                               hostId = listToMaybe $ root $/ elContent "HostId"
                               accessKeyId = listToMaybe $ root $/ elContent "AWSAccessKeyId"
                               stringToSign = do unprocessed <- listToMaybe $ root $/ elCont "StringToSignBytes"
                                                 bytes <- mapM readHex2 $ words unprocessed
                                                 return $ B.pack bytes
                           return DynamoDbError {
                                        ddbStatusCode = HTTP.responseStatus resp
                                      , ddbErrorCode = code
                                      , ddbErrorMessage = message
                                      , ddbErrorResource = resource
                                      , ddbErrorHostId = hostId
                                      , ddbErrorAccessKeyId = accessKeyId
                                      , ddbErrorStringToSign = stringToSign
                                      }

type CanonicalUserId = T.Text

data UserInfo
    = UserInfo {
        userId          :: CanonicalUserId
      , userDisplayName :: T.Text
      }
    deriving (Show)

parseUserInfo :: F.Failure XmlException m => Cu.Cursor -> m UserInfo
parseUserInfo el = do id_ <- force "Missing user ID" $ el $/ elContent "ID"
                      displayName <- force "Missing user DisplayName" $ el $/ elContent "DisplayName"
                      return UserInfo { userId = id_, userDisplayName = displayName }

data AttributeValue
    = AvBinary B.ByteString
    | AvBinarySet [B.ByteString]
    | AvNumber T.Text
    | AvNumberSet [T.Text]
    | AvString T.Text
    | AvStringSet [T.Text]
    deriving Show

renderAttributeValue :: AttributeValue -> Pair
renderAttributeValue (AvBinary blob)     = "B"  .= blob
renderAttributeValue (AvBinarySet blobs) = "BS" .= blobs
renderAttributeValue (AvNumber num)      = "N"  .= num
renderAttributeValue (AvNumberSet nums)  = "NS" .= nums
renderAttributeValue (AvString str)      = "S"  .= str
renderAttributeValue (AvStringSet strs)  = "SS" .= strs

data CannedAcl
    = AclPrivate
    | AclPublicRead
    | AclPublicReadWrite
    | AclAuthenticatedRead
    | AclLogDeliveryWrite
    deriving (Show)

writeCannedAcl :: CannedAcl -> T.Text
writeCannedAcl AclPrivate                = "private"
writeCannedAcl AclPublicRead             = "public-read"
writeCannedAcl AclPublicReadWrite        = "public-read-write"
writeCannedAcl AclAuthenticatedRead      = "authenticated-read"
writeCannedAcl AclLogDeliveryWrite       = "log-delivery-write"

data StorageClass
    = Standard
    | ReducedRedundancy
    deriving (Show)

parseStorageClass :: F.Failure XmlException m => T.Text -> m StorageClass
parseStorageClass "STANDARD"           = return Standard
parseStorageClass "REDUCED_REDUNDANCY" = return ReducedRedundancy
parseStorageClass s = F.failure . XmlException $ "Invalid Storage Class: " ++ T.unpack s

writeStorageClass :: StorageClass -> T.Text
writeStorageClass Standard          = "STANDARD"
writeStorageClass ReducedRedundancy = "REDUCED_REDUNDANCY"

type LocationConstraint = T.Text

locationUsClassic, locationUsWest, locationUsWest2, locationEu, locationApSouthEast, locationApNorthEast :: LocationConstraint
locationUsClassic = ""
locationUsWest = "us-west-1"
locationUsWest2 = "us-west-2"
locationEu = "EU"
locationApSouthEast = "ap-southeast-1"
locationApNorthEast = "ap-northeast-1"
